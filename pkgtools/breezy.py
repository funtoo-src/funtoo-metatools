#!/usr/bin/env python3

import os
import sys
import hashlib
from tornado import httpclient
from tornado.httpclient import HTTPRequest
import tornado.template
import logging
logging.basicConfig(level=logging.INFO)


def __init__(hub):
	pass

class BreezyError(Exception):
	pass

def make_ebuild_name(name=None, version=None, revision=None):
	if revision == 0:
		return "%s-%s.ebuild" % (name, version)
	else:
		return "%s-%s-r%s.ebuild" % (name, version, revision)

def create_ebuild(
		src: str = None,
		name: str = None,
		version: str = None,
		revision: int = None,
		artifacts: list = None,
		template_vars: dict = None):
	loader = tornado.template.Loader("templates")
	template = loader.load(src)
	outfn=make_ebuild_name(name=name, version=version, revision=revision)
	
	# generate template variables
	tvars = {}
	if template_vars is not None:
		tvars.update(template_vars)
	tvars["src"] = src
	tvars["name"] = name
	tvars["revision"] = revision
	tvars["artifacts"] = artifacts
	print("outfn", outfn)
	with open(outfn,"wb") as myf:
		myf.write(template.generate(**tvars))
	logging.info("Ebuild %s generated." % outfn)

class ArtifactFetcher:

	def __init__(self, artifact):
		self.artifact = artifact
		self.filename = artifact.split("/")[-1]
		self._fd = None
		self._sha512 = hashlib.sha512()
		self._blake2b = hashlib.blake2b()
		self._size = 0
		if os.path.exists(self.final_name):
			self.exists = True
		else:
			self.exists = False

	@property
	def temp_name(self):
		return "distfiles/%s.__download__" % self.filename

	@property
	def final_name(self):
		return "distfiles/%s" % self.filename

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
		if self._fd is None:
			self._fd = open(self.temp_name, "wb")
		self._fd.write(chunk)
		self._sha512.update(chunk)
		self._blake2b.update(chunk)
		self._size += len(chunk)
		sys.stdout.write(".")
		sys.stdout.flush()

	def fetch(self):
		if self.exists:
			self.update_digests()
			logging.warning("File %s exists (size %s); not fetching again." % ( self.filename, self.size ))
			return
		logging.info("Fetching %s..." % self.artifact)
		http_client = httpclient.HTTPClient()
		try:
			req = HTTPRequest(url=self.artifact, streaming_callback=self.on_chunk)
			http_client.fetch(req)
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
	src = None
	revision = 0
	destination = None

	def __init__(self):
		self.fetchers = []

	def setup(self):
		pass

	def fetch_all(self):
		os.makedirs("distfiles", exist_ok=True)
		for artifact in self.artifacts:
			af = ArtifactFetcher(artifact)
			try:
				af.fetch()
			except BreezyError as e:
				print("Fetch error for %s" % artifact)
				sys.exit(1)
			yield af

	def generate_metadata_for(self, fetchers):
		with open("Manifest", "w") as mf:
			for fetcher in fetchers:
				mf.write("DIST %s %s BLAKE2B %s SHA512 %s\n" % ( fetcher.filename, fetcher.size, fetcher.blake2b, fetcher.sha512 ))
		logging.info("Manifest generated.")

	def get_artifacts(self):
		self.generate_metadata_for(self.fetch_all())

	def generate(self):
		logging.info("Breezy 1.0")
		try:
			if self.cat is None:
				raise BreezyError("Please set 'cat' to the category name of this ebuild.")
			if self.name is None:
				raise BreezyError("Please set 'name' to the package name of this ebuild.")
			if self.src is None:
				self.src = self.name + ".tmpl"
			self.setup()
			create_ebuild(self.src, self.name, self.version, self.revision, self.artifacts)
			self.get_artifacts()
		except BreezyError as e:
			print(e)
			sys.exit(1)

# vim: ts=4 sw=4 noet
