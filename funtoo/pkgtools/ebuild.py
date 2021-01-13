#!/usr/bin/env python3

import os
import asyncio
import sys
from asyncio import Task
import jinja2
import logging
from collections import defaultdict

hub = None


class DigestFailure(Exception):
	def __init__(self, artifact=None, kind=None, expected=None, actual=None):
		self.artifact = artifact
		self.kind = kind
		self.expected = expected
		self.actual = actual

	@property
	def message(self):
		out = f"Digest Failure for {self.artifact.final_name}:\n"
		out += f"    Kind: {self.kind}\n"
		out += f"Expected: {self.expected}\n"
		out += f"  Actual: {self.actual}"
		return out


def __init__():
	hub.MANIFEST_LINES = defaultdict(set)


class BreezyError(Exception):
	def __init__(self, msg):
		self.msg = msg


class Fetchable:
	def __init__(self, url=None, **kwargs):
		self.url = url


class Artifact(Fetchable):
	def __init__(self, url=None, final_name=None, final_path=None, expect=None, **kwargs):
		super().__init__(url=url, **kwargs)
		self._final_name = final_name
		self._final_data = None
		self._final_path = final_path
		self.breezybuilds = []
		self.expect = expect

	@property
	def temp_path(self):
		return os.path.join(hub.MERGE_CONFIG.fetch_download_path, f"{self.final_name}.__download__")

	@property
	def extract_path(self):
		return hub.pkgtools.download.extract_path(self)

	@property
	def final_path(self):
		if self._final_path:
			return self._final_path
		else:
			return os.path.join(hub.MERGE_CONFIG.fetch_download_path, self.final_name)

	@property
	def final_name(self):
		if self._final_name is None:
			return self.url.split("/")[-1]
		else:
			return self._final_name

	async def fetch(self):
		await self.ensure_fetched()

	def is_fetched(self):
		return os.path.exists(self.final_path)

	@property
	def hashes(self):
		return self._final_data["hashes"]

	@property
	def size(self):
		return self._final_data["size"]

	def hash(self, h):
		return self._final_data["hashes"][h]

	@property
	def src_uri(self):
		if self._final_name is None:
			return self.url
		else:
			return self.url + " -> " + self._final_name

	def extract(self):
		return hub.pkgtools.download.extract(self)

	def cleanup(self):
		return hub.pkgtools.download.cleanup(self)

	def exists(self):
		return self.is_fetched()

	def record_final_data(self, final_data):
		self._final_data = final_data

	def validate_digests(self):
		"""
		The self.expect dictionary can be used to specify digests that we should expect to find in any completed
		or fetched Artifact. This method will throw a DigestFailure() exception when these expectations are not
		met.
		"""
		if self.expect is None:
			return
		for key, val in self.expect.items():
			if key == "size":
				if val != self.size:
					raise DigestFailure(artifact=self, kind="size", expected=val, actual=self.size)
			else:
				actual_hash = self.hash(key)
				if val != actual_hash:
					raise DigestFailure(artifact=self, kind=val, expected=val, actual=actual_hash)

	@property
	def fastpull_path(self):
		sh = self._final_data["hashes"]["sha512"]
		return hub.merge.fastpull.get_disk_path(sh)

	async def ensure_completed(self) -> bool:
		"""
		This function ensures that we can 'complete' the artifact -- meaning we have its hash data in the integrity database.
		If this hash data is not found, then we need to fetch the artifact.

		This function returns True if we do indeed have the artifact available to us, and False if there was some error
		which prevents this.

		So basically, this tries to use the distfile integrity database first to get the hash information, but will fall
		back to fetching. When this method is used instead of ensure_fetched(), we can get by without the file even
		existing on disk, as long as we have a distfile integrity entry. So it's preferred if that's all you need.
		"""

		if not len(self.breezybuilds):
			# Since we are using this outside of the context of an autogen, and there is no associated BreezyBuild,
			# using the distfile integrity database doesn't make sense. So just ensure the file is fetched:
			return await self.ensure_fetched()
		integrity_item = hub.merge.deepdive.get_distfile_integrity(self.breezybuilds[0].catpkg, distfile=self.final_name)
		if integrity_item is not None:
			self._final_data = integrity_item["final_data"]
			# Will throw an exception if our new final data doesn't match any expected values.
			self.validate_digests()
			return True
		else:
			return await self.ensure_fetched()

	async def ensure_fetched(self) -> bool:
		"""
		This function ensures that the artifact is 'fetched' -- in other words, it exists locally. This means we can
		calculate its hashes or extract it.

		Returns a boolean with True indicating success and False failure.
		"""
		if self.is_fetched():
			if self._final_data is not None:
				# Nothing to do.
				return True
			else:
				# This condition handles a situation where the distfile integrity database has been wiped. We need to
				# re-populate the data. We already have the file.
				if len(self.breezybuilds):
					self._final_data = hub.merge.deepdive.get_distfile_integrity(
						self.breezybuilds[0].catpkg, distfile=self.final_name
					)
					if self._final_data is None:
						self._final_data = hub.pkgtools.download.calc_hashes(self.final_path)
						# Will throw an exception if our new final data doesn't match any expected values.
						self.validate_digests()
						hub.merge.deepdive.store_distfile_integrity(self.breezybuilds[0].catpkg, self.final_name, self._final_data)
				else:
					self._final_data = hub.pkgtools.download.calc_hashes(self.final_path)
					# Will throw an exception if our new final data doesn't match any expected values.
					self.validate_digests()
				return True
		else:
			active_dl = hub.pkgtools.download.get_download(self.final_name)
			if active_dl is not None:
				# Active download -- wait for it to finish:
				logging.info(f"Waiting for {self.final_name} download to finish")
				success = await active_dl.wait_for_completion(self)
				if success:
					self._final_data = active_dl.final_data
			else:
				# No active download for this file -- start one:
				dl_file = hub.pkgtools.download.Download(self)
				success = await dl_file.download()
			if success:
				# Will throw an exception if our new final data doesn't match any expected values.
				self.validate_digests()
			return success


def aggregate(meta_list):
	out_list = []
	for item in meta_list:
		if isinstance(item, list):
			out_list += item
		else:
			out_list.append(item)
	return out_list


class BreezyBuild:

	cat = None
	name = None
	path = None
	template = None
	version = None
	revision = 0
	source_tree = None
	output_tree = None
	template_args = None

	def __init__(
		self,
		artifacts: list = None,
		template: str = None,
		template_text: str = None,
		template_path: str = None,
		**kwargs,
	):
		self.source_tree = hub.CONTEXT
		self.output_tree = hub.OUTPUT_CONTEXT
		self._pkgdir = None
		self.template_args = kwargs
		for kwarg in ["cat", "name", "version", "revision", "path"]:
			if kwarg in kwargs:
				setattr(self, kwarg, kwargs[kwarg])
		self.template = template
		self.template_text = template_text
		if template_path is None:
			if "path" in self.template_args:
				# If we have a pkginfo['path'], this gives us our current processing path.
				# Use this as a base for our default template path.
				self._template_path = os.path.join(self.template_args["path"] + "/templates")
			else:
				# This is a no-op, but wit this set to None, we will use the template_path()
				# property to get the value, which will be relative to the repo root and based
				# on the setting of name and category.
				self._template_path = None
		else:
			# A manual template path was specified.
			self._template_path = template_path
		if self.template_text is None and self.template is None:
			self.template = self.name + ".tmpl"
		if artifacts is None:
			self.artifacts = []
		else:
			self.artifacts = artifacts
		self.template_args["artifacts"] = artifacts

	async def setup(self):
		"""
		This method performs some special setup steps. We tend to treat Artifacts as stand-alone objects -- and they
		can be -- such as if you instantiate an Artifact in `generate()` and fetch it because you need to extract it
		and look inside it.

		But when associated with a BreezyBuild, as is commonly the case, this means that there is a relationship between
		the Artifact and the BreezyBuild.

		In this scenario, we know that the Artifact is associated with a catpkg, and will be written out to a Manifest.
		So this means we want to create some associations. We want to record that the Artifact is associated with the
		catpkg of this BreezyBuild. We use this for writing new entries to the Distfile Integrity database for
		to-be-fetched artifacts.

		The `ensure_completed()` call will ensure the Artifact has hashes from the Distfile Integrity Database or is
		fetched and hashes calculated.

		TODO: if the file exists on disk, but there is no Distfile Integrity entry, we need to make sure that hashes
		      are not just calculated -- the distfile integrity entry should be created as well.

		"""
		assert id(asyncio.get_running_loop()) == id(hub.THREAD_CTX.loop)

		fetch_tasks_dict = {}

		for artifact in self.artifacts:
			if type(artifact) != Artifact:
				artifact = Artifact(**artifact)

			# This records that the artifact is used by this catpkg, because an Artifact can be shared among multiple
			# catpkgs. This is used for the integrity database writes:

			if self not in artifact.breezybuilds:
				artifact.breezybuilds.append(self)

			async def lil_coroutine(a):
				status = await a.ensure_completed()
				return a, status

			fetch_tasks_dict[artifact] = asyncio.Task(lil_coroutine(artifact))

		# Wait for any artifacts that are still fetching:
		results, exceptions = await hub.pkgtools.autogen.gather_pending_tasks(fetch_tasks_dict.values())
		completion_list = aggregate(results)
		for artifact, status in completion_list:
			if status is False:
				logging.error(f"Artifact for url {artifact.url} referenced in {artifact.catpkgs} could not be fetched.")
				sys.exit(1)

	def push(self):
		#
		# https://stackoverflow.com/questions/1408171/thread-local-storage-in-python

		async def wrapper(self):
			await self.generate()
			return self

		# This will cause the BreezyBuild to start autogeneration immediately, appending the task to the thread-
		# local context so we can grab the result later. The return value will be the BreezyBuild object itself,
		# thanks to the wrapper.
		bzb_task = Task(wrapper(self))
		hub.THREAD_CTX.running_breezybuilds.append(bzb_task)

	@property
	def pkgdir(self):
		if self._pkgdir is None:
			self._pkgdir = os.path.join(self.source_tree.root, self.cat, self.name)
			os.makedirs(self._pkgdir, exist_ok=True)
		return self._pkgdir

	@property
	def output_pkgdir(self):
		if self._pkgdir is None:
			self._pkgdir = os.path.join(self.output_tree.root, self.cat, self.name)
			os.makedirs(self._pkgdir, exist_ok=True)
		return self._pkgdir

	@property
	def ebuild_name(self):
		if self.revision == 0:
			return "%s-%s.ebuild" % (self.name, self.version)
		else:
			return "%s-%s-r%s.ebuild" % (self.name, self.version, self.revision)

	@property
	def ebuild_path(self):
		return os.path.join(self.pkgdir, self.ebuild_name)

	@property
	def output_ebuild_path(self):
		return os.path.join(self.output_pkgdir, self.ebuild_name)

	@property
	def catpkg(self):
		return self.cat + "/" + self.name

	def __getitem__(self, key):
		return self.template_args[key]

	@property
	def catpkg_version_rev(self):
		if self.revision == 0:
			return self.cat + "/" + self.name + "-" + self.version
		else:
			return self.cat + "/" + self.name + "-" + self.version + "-r%s" % self.revision

	@property
	def template_path(self):
		if self._template_path:
			return self._template_path
		tpath = os.path.join(self.source_tree.root, self.cat, self.name, "templates")
		return tpath

	async def record_manifest_lines(self):
		"""
		This method records literal Manifest output lines which will get written out later, because we may
		not have *all* the Manifest lines we need to write out until autogen is fully complete.
		"""
		if not len(self.artifacts):
			return

		key = self.output_pkgdir + "/Manifest"

		for artifact in self.artifacts:
			success = await artifact.ensure_completed()
			if not success:
				raise BreezyError(f"Something prevented us from storing Manifest data for {key}.")
			hub.MANIFEST_LINES[key].add(
				"DIST %s %s BLAKE2B %s SHA512 %s\n"
				% (artifact.final_name, artifact.size, artifact.hash("blake2b"), artifact.hash("sha512"))
			)

	def create_ebuild(self):
		if not self.template_text:
			template_file = os.path.join(self.template_path, self.template)
			try:
				with open(template_file, "r") as tempf:
					template = jinja2.Template(tempf.read())
			except FileNotFoundError as e:
				logging.error(f"Could not find template: {template_file}")
				raise BreezyError(f"Template file not found: {template_file}")
		else:
			template = jinja2.Template(self.template_text)

		with open(self.output_ebuild_path, "wb") as myf:
			myf.write(template.render(**self.template_args).encode("utf-8"))
		logging.info("Created: " + os.path.relpath(self.output_ebuild_path))

	async def generate(self):
		"""
		This is an async method that does the actual creation of the ebuilds from templates. It also handles
		initialization of Artifacts (indirectly) and could result in some HTTP fetching. If you call
		``myebuild.push()``, this is the task that gets pushed onto the task queue to run in parallel.
		If you don't call push() on your BreezyBuild, then you could choose to call the generate() method
		directly instead. In that case it will run right away.
		"""
		if self.cat is None:
			raise BreezyError("Please set 'cat' to the category name of this ebuild.")
		if self.name is None:
			raise BreezyError("Please set 'name' to the package name of this ebuild.")
		await self.setup()
		self.create_ebuild()
		await self.record_manifest_lines()
		return self


# vim: ts=4 sw=4 noet
