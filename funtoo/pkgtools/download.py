#!/usr/bin/env python3

import asyncio
import hashlib
import logging
import os
import sys
from asyncio import Semaphore, Lock
from subprocess import getstatusoutput
from contextlib import asynccontextmanager

"""
This sub deals with the higher-level logic related to downloading of distfiles. Where the 'fetch.py'
sub deals with grabbing HTTP data from APIs, this is much more geared towards grabbing tarballs that
are bigger, and organizing them into a distfiles directory. This includes calculating cryptographic
hashes on the resultant downloads and ensuring they match what we expect.

The implementation is based around a class called `Download`.

Because autogen uses asyncio, it's possible for two autogens to try downloading the same file
at the same time. If they create a `Download` object, this special class will do the magic of looking
for any active downloads of the same file, and if one exists, it will not fire
off a new download but instead wait for the existing download to complete. So the 'downloader'
(code trying to download the file) can remain ignorant of the fact that the download was already
started previously.

This allows asyncio downloads of potentially identical files to work without complication in
the autogen.py files or generators so that this complexity does not have to be dealt with by those
who are simply writing autogens.
"""
hub = None
DL_ACTIVE_LOCK = Lock()
DL_ACTIVE = dict()
DOWNLOAD_SLOT = {}


async def acquire_download_slot():
	"""
	This should ensure that the Semaphore is created INSIDE the ioloop, which appears to be necessary:

	https://stackoverflow.com/a/53724990

	Did all this shit get inherited from Java?
	"""
	global DOWNLOAD_SLOT
	loop = asyncio.get_running_loop()
	if id(loop) not in DOWNLOAD_SLOT:
		DOWNLOAD_SLOT[id(loop)] = Semaphore(value=24)
	return DOWNLOAD_SLOT[id(loop)]


def __init__():
	hub.CHECK_DISK_HASHES = False


@asynccontextmanager
async def start_download(download):
	"""
	Automatically record the download as being active, and remove from our list when complete.
	"""
	try:
		async with DL_ACTIVE_LOCK:
			DL_ACTIVE[download.final_name] = download
		yield
	finally:
		async with DL_ACTIVE_LOCK:
			del DL_ACTIVE[download.final_name]


def get_download(final_name):
	"""
	Get a download object for the file we're interested in if one is already being downloaded.
	"""
	if final_name in DL_ACTIVE:
		return DL_ACTIVE[final_name]
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
		self.futures = []

	def add_artifact(self, artifact):
		self.artifacts.append(artifact)

	def wait_for_completion(self, artifact):
		self.artifacts.append(artifact)
		fut = hub.LOOP.create_future()
		self.futures.append(fut)
		return fut

	async def download(self) -> bool:
		"""
		This method attempts to start a download. It hooks into ``download_slot`` which is used to limit the number
		of simultaneous downloads.

		Upon success, it will also record 'distfile integrity' entries into MongoDB on completion, and call any
		download completion hook for fastpull (which is used to insert the resultant file into fastpull.)

		Will return True on success and False on failure. Will also ensure that if others are waiting on this
		file, they will get True on success and False on failure (self.futures holds futures for others waiting
		on this file, and we will future.set_result() with the boolean return code as well.)
		"""
		print("starting download")
		slot = await acquire_download_slot()
		async with slot:
			async with start_download(self):
				success = True
				try:
					final_data = await _download(self.artifacts[0])
				except hub.pkgtools.fetch.FetchError as fe:
					logging.error(fe)
					success = False

				if success:
					integrity_keys = {}
					for artifact in self.artifacts:
						artifact.record_final_data(final_data)
						for breezybuild in artifact.breezybuilds:
							integrity_keys[(breezybuild.catpkg, artifact.final_name)] = True

					# For every final_name referenced by a catpkg, create a distfile integrity entry. We use integrity_keys to
					# avoid duplicate records.

					for catpkg, final_name in integrity_keys.keys():
						hub.merge.deepdive.store_distfile_integrity(catpkg, final_name, final_data)

		for future in self.futures:
			future.set_result(success)

		return success


def extract_path(artifact):
	return os.path.join(hub.MERGE_CONFIG.temp_path, "artifact_extract", artifact.final_name)


async def _download(artifact):
	"""

	This function is used to download tarballs and other artifacts. Because files can be large,
	it uses a streaming download so the entire file doesn't need to be cached in memory. Hashes
	of the downloaded file are computed as the file is in transit.

	Upon success, the function will update the Artifact's hashes dict to contain hashes and
	filesize of the downloaded artifact.

	Will raise hub.pkgtools.fetch.FetchError if there was some kind of error downloading. Caller
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

		await hub.pkgtools.http.http_fetch_stream(artifact.url, on_chunk)

		sys.stdout.write("x")
		sys.stdout.flush()
		fd.close()
		os.link(temp_path, final_path)

		final_data = {"size": filesize, "hashes": {}, "path": final_path}

		for h in HASHES:
			final_data["hashes"][h] = hashes[h].hexdigest()

	# TODO: this is likely a good place for GPG verification. Implement.
	finally:
		if os.path.exists(temp_path):
			os.unlink(temp_path)

	return final_data


def cleanup(artifact):
	# TODO: check for path stuff like ../.. in final_name to avoid security issues.
	getstatusoutput("rm -rf " + os.path.join(hub.MERGE_CONFIG.temp_path, "artifact_extract", artifact.final_name))


def extract(artifact):
	# TODO: maybe refactor these next 2 lines
	if not artifact.exists:
		artifact.fetch()
	ep = extract_path(artifact)
	os.makedirs(ep, exist_ok=True)
	cmd = "tar -C %s -xf %s" % (ep, artifact.final_path)
	s, o = getstatusoutput(cmd)
	if s != 0:
		raise hub.pkgtools.ebuild.BreezyError("Command failure: %s" % cmd)


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
