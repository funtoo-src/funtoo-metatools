#!/usr/bin/env python3

import asyncio
import hashlib
import logging
import os
import sys
from threading import Semaphore, Lock
from subprocess import getstatusoutput
from contextlib import asynccontextmanager

import dyne.org.funtoo.metatools.merge as merge
import dyne.org.funtoo.metatools.pkgtools as pkgtools

"""
This sub deals with the higher-level logic related to downloading of distfiles. Where the 'fetch.py'
sub deals with grabbing HTTP data from APIs, this is much more geared towards grabbing tarballs that
are bigger, and organizing them into a distfiles directory. This includes calculating cryptographic
hashes on the resultant downloads and ensuring they match what we expect.

The implementation is based around a class called `Download`.

Why do we have a class called 'Download'? Imagine we have an autogen, and it has two Artifacts
referencing the same file (this can and does happen.) Do we want to download the same file twice?
No -- it would be far better if we downloaded the file once, and then provided the results to
each Artifact, saying in effect 'here is the file you wanted to download.' This is why the
Download class exists.

Because autogen uses asyncio, it's possible for two autogens to try downloading the same file
at the same time. If they create a `Download` object, this special class will do the magic of looking
for any active downloads of the same file, and if one exists, it will not fire
off a new download but instead wait for the existing download to complete. So the 'downloader'
(code trying to download the file) can remain ignorant of the fact that the download was already
started previously.

Locking Code
============

The locking code below deserves some explanation. DL_ACTIVE tracks all the active downloads for
*all* threads that are running. DL_ACTIVE_LOCK is a lock we use to access this dictionary, when
we want to read or modify it.

DOWNLOAD_SLOT is the mechanism we used to ensure we only have a certain number (specified by the
value= parameter) of downloads active at once. Each active download will acquire a slot. When all
slots are exhausted, any pending downloads will wait for an active slot before they can begin.
"""

DL_ACTIVE_LOCK = Lock()
DL_ACTIVE = dict()
DOWNLOAD_SLOT = Semaphore(value=200)


@asynccontextmanager
async def acquire_download_slot(download):
	"""
	This code originally tried to do this, but it would deadlock::

	  with DOWNLOAD_SLOT:
        yield

	This code ^^ will deadlock as hit the max semaphore value. The reason? When we hit the max value, it will block
	for a download slot in the current thread will FREEZE our thread's ioloop, which will prevent another asyncio
	task from executing which needs to *release* the download slot -- thus the deadlock.

	So instead of using this approach, we will attempt to acquire a download slot in a non-blocking fashion. If we
	succeed -- great. If not, we will asyncio loop to repeatedly attempt to acquire the slot with a slight delay
	between each attempt. This ensures that the ioloop can continue to function and release any download slots while
	we wait.
	"""
	global DOWNLOAD_SLOT
	try:
		while True:
			success = DOWNLOAD_SLOT.acquire(blocking=False)
			if not success:
				await asyncio.sleep(0.1)
				continue
			yield
			break
	finally:
			DOWNLOAD_SLOT.release()



@asynccontextmanager
async def start_download(download):
	"""
	Automatically record the download as being active, and remove from our list when complete.

	While waiting for DL_ACTIVE_LOCK will FREEZE the current thread's ioloop, this is OK because we immediately release
	the lock after inspecting/modifying the protected resource (DL_ACTIVE in this case.)
	"""
	try:
		with DL_ACTIVE_LOCK:
			DL_ACTIVE[download.final_name] = download
		yield
	finally:
		with DL_ACTIVE_LOCK:
			if download.final_name in DL_ACTIVE:
				del DL_ACTIVE[download.final_name]


def get_download(final_name):
	"""
	Get a download object for the file we're interested in if one is already being downloaded.
	"""
	with DL_ACTIVE_LOCK:
		if final_name in DL_ACTIVE:
			return DL_ACTIVE[final_name]
		else:
			return None


HASHES = ["sha256", "sha512", "blake2b"]

# TODO: implement different download strategies with different levels of security. Maybe as a
#       declarative pipeline.


class Download:

	"""
	When we need to download an artifact, we create a download. Multiple, co-existing Artifact
	objects can reference the same file. Rather than have them try to download the same file
	at the same time, they leverage a "Download" which eliminates conflicts and manages the
	retrieval of the file.

	The Download object will record all Artifacts that need this file, and arbitrate the download
	of this file and update the Artifacts with the completion data when the download is complete.

	A Download will be shared only if the Artifacts fetching the file are storing it as the same
	final_name. So it's possible that if the final_name differs that files could be theoretically
	downloaded multiple times or simultaneously and redundantly (but this rarely if ever happens,
	just worth mentioning and a possible improvement in the future.)

	The
	"""

	def __init__(self, artifact):
		self.final_name = artifact.final_name
		self.url = artifact.url
		self.artifacts = [artifact]
		self.final_data = None
		self.futures = []

	def add_artifact(self, artifact):
		self.artifacts.append(artifact)

	def wait_for_completion(self, artifact):
		self.artifacts.append(artifact)
		fut = hub.LOOP.create_future()
		self.futures.append(fut)
		return fut

	async def download(self, throw=False) -> bool:
		"""
		This method attempts to start a download. It hooks into ``download_slot`` which is used to limit the number
		of simultaneous downloads.

		Upon success, it will also record 'distfile integrity' entries into MongoDB on completion, and call any
		download completion hook for fastpull (which is used to insert the resultant file into fastpull.)

		Will return True on success and False on failure. Will also ensure that if others are waiting on this
		file, they will get True on success and False on failure (self.futures holds futures for others waiting
		on this file, and we will future.set_result() with the boolean return code as well.)
		"""
		async with acquire_download_slot(self):
			async with start_download(self):
				success = True
				try:
					self.final_data = await _download(self.artifacts[0], retry=not throw)
				except pkgtools.fetch.FetchError as fe:
					logging.error(fe)
					if throw:
						raise fe
					else:
						logging.error(f"FETCH ERROR: {self.artifacts[0].url}")
						success = False

				if success:
					integrity_keys = {}
					for artifact in self.artifacts:
						artifact.record_final_data(self.final_data)
						for breezybuild in artifact.breezybuilds:
							integrity_keys[(breezybuild.catpkg, artifact.final_name)] = True

					# For every final_name referenced by a catpkg, create a distfile integrity entry. We use integrity_keys to
					# avoid duplicate records.

					for catpkg, final_name in integrity_keys.keys():
						merge.deepdive.store_distfile_integrity(catpkg, final_name, self.final_data)

		for future in self.futures:
			future.set_result(success)

		return success


def extract_path(artifact):
	return os.path.join(merge.model.MERGE_CONFIG.temp_path, "artifact_extract", artifact.final_name)


async def _download(artifact, retry=True):
	"""

	This function is used to download tarballs and other artifacts. Because files can be large,
	it uses a streaming download so the entire file doesn't need to be cached in memory. Hashes
	of the downloaded file are computed as the file is in transit.

	Upon success, the function will update the Artifact's hashes dict to contain hashes and
	filesize of the downloaded artifact.

	Will raise pkgtools.fetch.FetchError if there was some kind of error downloading. Caller
	needs to catch and handle this.

	"""
	logging.info(f"Fetching {artifact.url}...")

	temp_path = artifact.temp_path
	os.makedirs(os.path.dirname(temp_path), exist_ok=True)
	final_path = artifact.final_path

	try:
		fd = open(temp_path, "wb")
		hashes = {}

		for h in HASHES:
			hashes[h] = getattr(hashlib, h)()
		filesize = 0

		def on_chunk(chunk):
			# See https://stackoverflow.com/questions/5218895/python-nested-functions-variable-scoping
			nonlocal filesize
			fd.write(chunk)
			for hash in HASHES:
				hashes[hash].update(chunk)
			filesize += len(chunk)
			sys.stdout.write(".")
			sys.stdout.flush()

		await pkgtools.http.http_fetch_stream(artifact.url, on_chunk, retry=retry, extra_headers=artifact.extra_http_headers)

		sys.stdout.write("x")
		sys.stdout.flush()
		fd.close()
		try:
			os.link(temp_path, final_path)
		except (FileExistsError, FileNotFoundError):
			# FL-8301: address possible race condition
			pass
		final_data = {"size": filesize, "hashes": {}, "path": final_path}

		for h in HASHES:
			final_data["hashes"][h] = hashes[h].hexdigest()

	# TODO: this is likely a good place for GPG verification. Implement.
	finally:
		if os.path.exists(temp_path):
			try:
				os.unlink(temp_path)
			except FileNotFoundError:
				# FL-8301: address possible race condition
				pass

	return final_data


def cleanup(artifact):
	# TODO: check for path stuff like ../.. in final_name to avoid security issues.
	getstatusoutput("rm -rf " + os.path.join(merge.model.MERGE_CONFIG.temp_path, "artifact_extract", artifact.final_name))


def extract(artifact):
	if not artifact.exists:
		artifact.fetch()
	ep = artifact.extract_path
	os.makedirs(ep, exist_ok=True)
	if artifact.final_name.endswith(".zip"):
		cmd = f"unzip {ep} -d {artifact.final_path}"
	else:
		cmd = f"tar -C {ep} -xf {artifact.final_path}"
	s, o = getstatusoutput(cmd)
	if s != 0:
		raise pkgtools.ebuild.BreezyError("Command failure: %s" % cmd)


def calc_hashes(fn):
	hashes = {}
	for h in HASHES:
		hashes[h] = getattr(hashlib, h)()
	filesize = 0
	with open(fn, "rb") as myf:
		while True:
			data = myf.read(1280000)
			if not data:
				break
			for h in hashes:
				hashes[h].update(data)
			filesize += len(data)
	final_data = {"size": filesize, "hashes": {}, "path": fn}
	for h in HASHES:
		final_data["hashes"][h] = hashes[h].hexdigest()
	return final_data


async def check_hashes(old_hashes, new_hashes):
	"""
	This method compares two sets of hashes passed to it and throws an exception if they don't match.
	"""
	failures = []
	for h in HASHES:
		old = old_hashes[h]
		new = new_hashes[h]
		if old != new:
			failures.append((h, old, new))
	return failures


# vim: ts=4 sw=4 noet
