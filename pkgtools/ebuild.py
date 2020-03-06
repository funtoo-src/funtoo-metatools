#!/usr/bin/env python3

import os
import sys
import hashlib
import asyncio
from tornado import httpclient
from tornado.httpclient import HTTPRequest
import tornado.template
import logging
logging.basicConfig(level=logging.INFO)

def __init__(hub):
	pass

class BreezyError(Exception):
	pass

class ArtifactFetcher:

	def __init__(self, artifact, temp_path="/var/tmp/distfiles"):
		self.artifact = artifact
		self.filename = artifact.split("/")[-1]
		self._fd = None
		self._sha512 = hashlib.sha512()
		self._blake2b = hashlib.blake2b()
		self._size = 0
		self.temp_path = temp_path
		os.makedirs(self.temp_path, exist_ok=True)

	@property
	def exists(self):
		return os.path.exists(self.final_name)

	@property
	def temp_name(self):
		return os.path.join(self.temp_path, "%s.__download__" % self.filename)

	@property
	def final_name(self):
		return os.path.join(self.temp_path, "%s" % self.filename)

	@property
	def sha512(self):
		return self._sha512.hexdigest()

	@property
	def blake2b(self):
		return self._blake2b.hexdigest()

	@property
	def size(self):
		return self._size

	def update_digests(self):
		logging.info("Calculating digests for %s..." % self.final_name)
		with open(self.final_name, 'rb') as myf:
			while True:
				data = myf.read(1280000)
				if not data:
					break
				self._sha512.update(data)
				self._blake2b.update(data)
				self._size += len(data)

	def on_chunk(self, chunk):
		self._fd.write(chunk)
		self._sha512.update(chunk)
		self._blake2b.update(chunk)
		self._size += len(chunk)
		sys.stdout.write(".")
		sys.stdout.flush()

	async def fetch(self):
		if self.exists:
			self.update_digests()
			logging.warning("File %s exists (size %s); not fetching again." % ( self.filename, self.size ))
			return
		logging.info("Fetching %s..." % self.artifact)
		if self._fd is None:
			self._fd = open(self.temp_name, "wb")
		http_client = httpclient.AsyncHTTPClient()
		try:
			req = HTTPRequest(url=self.artifact, streaming_callback=self.on_chunk)
			await http_client.fetch(req)
		except httpclient.HTTPError as e:
			raise BreezyError("Fetch Error")
		http_client.close()
		if self._fd is not None:
			self._fd.close()
			os.link(self.temp_name, self.final_name)
			os.unlink(self.temp_name)


class BreezyBuild:

	cat = None
	name = None
	template = None
	version = None
	revision = 0
	destination = None
	template = None

	def __init__(self, dest=None):
		self.destination = dest
		self.fetchers = []
		self._pkgdir = None
		if self.template is None:
			self.template = self.name + ".tmpl"

	async def setup(self):
		pass

	async def fetch_all(self):
		results = []
		for artifact in self.artifacts:
			af = ArtifactFetcher(artifact)
			results.append(af)
			try:
				await af.fetch()
			except BreezyError as e:
				print("Fetch error for %s" % artifact)
				sys.exit(1)
		return results

	@property
	def pkgdir(self):
		if self._pkgdir is None:
			self._pkgdir = os.path.join(self.destination, self.cat, self.name)
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
	def template_path(self):
		tpath = os.path.join(self.destination, self.cat, self.name, "templates")
		os.makedirs(tpath, exist_ok=True)
		return tpath

	def generate_metadata_for(self, fetchers):
		with open(self.pkgdir + "/Manifest", "w") as mf:
			for fetcher in fetchers:
				mf.write("DIST %s %s BLAKE2B %s SHA512 %s\n" % ( fetcher.filename, fetcher.size, fetcher.blake2b, fetcher.sha512 ))
		logging.info("Manifest generated.")

	async def get_artifacts(self):
		self.generate_metadata_for(await self.fetch_all())


	def create_ebuild(self, template_vars: dict = None):
		# TODO: fix path on next line to point somewhere logical.
		loader = tornado.template.Loader(self.template_path)
		template = loader.load(self.template)
		# generate template variables
		tvars = {}
		if template_vars is not None:
			tvars.update(template_vars)
		tvars["template"] = self.template
		tvars["name"] = self.name
		tvars["cat"] = self.cat
		tvars["version"] = self.version
		tvars["revision"] = self.revision
		tvars["artifacts"] = self.artifacts
		with open(self.ebuild_path, "wb") as myf:
			myf.write(template.generate(**tvars))
		logging.info("Ebuild %s generated." % self.ebuild_path)

	async def generate(self):
		logging.info("Breezy 1.0")
		try:
			if self.cat is None:
				raise BreezyError("Please set 'cat' to the category name of this ebuild.")
			if self.name is None:
				raise BreezyError("Please set 'name' to the package name of this ebuild.")
			await self.setup()
			self.create_ebuild()
			await self.get_artifacts()
		except BreezyError as e:
			print(e)
			sys.exit(1)

# vim: ts=4 sw=4 noet
