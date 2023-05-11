#!/usr/bin/env python3

from __future__ import annotations
import logging
from datetime import datetime
from typing import Tuple

from metatools.blos import BaseLayerObjectStore
from metatools.fastpull.spider import FetchRequest, Download
from metatools.tree import run_bg
from metatools.store import Store, FileStorageBackend, DerivedKey, StoreObject

log = logging.getLogger('metatools.autogen')


class FileIntegrityError(Exception):
	pass


class IntegrityScope:

	def __init__(self, parent, scope, validate_hashes=None):
		if validate_hashes is None:
			validate_hashes = {"sha512"}
		self.validate_hashes = validate_hashes
		self.parent = parent
		self.scope = scope
		self.store = Store(
			collection="integrity",
			prefix=self.scope,
			backend=FileStorageBackend(db_base_path=self.parent.db_base_path),
			key_spec=DerivedKey(["url"])
		)
		self.dynamic = Store(
			collection="dynamic",
			prefix=self.scope,
			backend=FileStorageBackend(db_base_path=self.parent.db_base_path),
			key_spec=DerivedKey(["key"])
		)

	async def get_file_by_url(self, request: FetchRequest) -> StoreObject:
		"""

		This method will attempt to return a StoreObject reference to binary data which is associated with
		a URL, referenced by ``request.url``, in this ``IntegrityScope``. Typically, this means that a
		tarball is being requested, and we may have this tarball already available locally, or we may
		need to use the ``Spider`` to download it. This is the method used to retrieve ``Artifact``s
		from the interwebs.

		If the object associated with the URL is available locally, a ``BLOSObject`` will be returned
		that references this object.

		In the case that the URL has not yet been retrieved and, it will be downloaded, and inserted into
		the BLOS, and a reference to this inserted file will be returned.

		If the file is currently in the process of being downloaded, but this download has not completed
		yet, this call will block until the download has completed, and then a reference to the resultant
		BLOSObject will be returned.

		The specific flow that will be followed is:

		We'll first look at our IntegrityScope and see if we have an existing reference ('ref') to this file,
		by looking it up by the ``request.url``. We may have a record for it in our IntegrityScope, in which case we can
		try to retrieve it from the BLOS, using the ``sha512`` hash as a 'link' to the file in the BLOS. If we have
		a ref, but the BLOS object is missing for some reason, we will raise an exception as this can indicate
		a problem with our BLOS and we should not try to fix this automatically.

		If we have no local copy in the BLOS, we will definitely have to start a fresh download. We will leverage
		the data in the FetchRequest and use the Spider to launch a streaming HTTP download. This will also transparently
		handle the situation where we are *currently downloading* this file, for another asyncio task, and will
		internally wait for this download to finish rather than launching a new download.

		If our request actually initiated a fresh download, then when the download completes, the
		``self.parent.fetch_completion_callback`` we specified will cause some actions to happen. The downloaded
		file will be injected into the BLOS.

		Whether we initiated the download or not, we will get back the result of this pipeline and get the
		BLOSObject for the inserted object returned to us. We will pass this object back to the caller.

		If there is some kind of FetchError that could not be managed, it will be raised by the Spider, so that
		the root cause of the fetch error can be propagated to the caller.
		"""

		existing_ref: StoreObject = self.store.read({"url": request.url})
		if existing_ref is not None:
			obj = self.parent.blos.read({"hashes.sha512": existing_ref.data['sha512']})
			if obj is not None:
				log.debug(
					f"IntegrityScope:{self.scope}._get_file_by_url_new: existing object found for ref {request.url}")
				return obj
		new_ref = await self.parent.spider.download(request,
		                                            completion_pipeline=[
														self.parent.verify_callback,
														self.parent.fetch_completion_callback])
		assert isinstance(new_ref, StoreObject)
		self.store.write(
			{"url": request.url, "sha512": new_ref.data["hashes"]["sha512"], "updated_on": datetime.utcnow()})
		return new_ref

	def get_file_dynamic(self, key_dict: dict) -> Tuple[StoreObject, dict] | Tuple[None, None]:
		existing_ref: StoreObject = self.dynamic.read({"key": key_dict})
		if existing_ref:
			log.debug(f"get_file_dynamic: existing_ref: {existing_ref.data}")
		if existing_ref is not None:
			obj = self.parent.blos.read({"hashes.sha512": existing_ref.data['hashes']['sha512']})
			if obj is not None:
				if "metadata" in existing_ref.data:
					metadata = existing_ref.data["metadata"]
				else:
					metadata = {}
				return obj, metadata
		return None, None

	def store_file_dynamic(self, key_dict: dict, obj_path, metadata: dict = None):
		store_obj = self.parent.blos.insert_blob(obj_path)
		self.dynamic.write({"key": key_dict, "hashes": store_obj.data["hashes"], "metadata": metadata if metadata else {}})


class IntegrityDatabase:

	def __init__(self, db_base_path, blos: BaseLayerObjectStore = None, spider=None, hashes: set = None):
		"""
		``blos`` is an instance of a Base Layer Object Store (used to store distfiles, indexed by their hashes,
		and also takes care of all integrity checking tasks for us.

		``spider`` is an instance of a WebSpider, which handles all downloading tasks for us.

		``hashes`` is a set of cryptographic hashes

		The fastpull database uses sha512 as a 'linking mechanism' to the Base Layer Object Store (BLOS). So only
		one hash needs to be recorded, since this is not an exhaustive integrity check (that is performed by the
		BLOS itself upon retrieval). This is stored in the 'sha512' key, which is not placed inside 'hashes' like
		it is in the BLOS. But we do not create an index for it, since we don't encourage retrieval of objects from
		fastpull by their hash. They should be retrieved by target URL (and scope). So we always want to retrieve
		the ref by the URL, then from the returned record, use the sha512 to see if the BLOS entry exists.
		"""
		assert hashes
		self.db_base_path = db_base_path
		self.hashes = hashes
		self.blos: BaseLayerObjectStore = blos
		self.spider = spider
		self.scopes = {}

	def get_scope(self, scope_id):
		"""
		This method returns an 'IntegrityScope', which is basically like a session for doit (autogen) to
		associate URLs with entries in the BLOS, or Base Layer Object Store.
		"""
		if scope_id not in self.scopes:
			self.scopes[scope_id] = IntegrityScope(self, scope_id)
		log.debug(f"FastPull Integrity Scope: {scope_id}")
		return self.scopes[scope_id]

	async def verify_callback(self, download: Download) -> Download:
		fn = download.request.filename
		run_cmd = None
		arc_desc = None
		if fn.endswith(".tar.gz"):
			run_cmd = "tar tzf {archive}"
			arc_desc = "tar.gz"
		elif fn.endswith(".tar.bz2"):
			run_cmd = "tar tjf {archive}"
			arc_desc = "tar.bz2"
		elif fn.endswith(".tar.xz"):
			run_cmd = "tar tJf {archive}"
			arc_desc = "tar.xz"
		elif fn.endswith(".tar.zst"):
			run_cmd = "tar -t --zstd -f {archive}"
			arc_desc = "tar.xz"
		elif fn.endswith(".gz"):
			run_cmd = "gzip -dc {archive} > /dev/null"
			arc_desc = "gzip"
		elif fn.endswith(".bz2"):
			run_cmd = "bzip2 -dc {archive} > /dev/null"
			arc_desc = "bzip2"
		elif fn.endswith(".xz"):
			run_cmd = "xz -dc {archive} > /dev/null"
			arc_desc = "xz"
		if run_cmd:
			proc, out = await run_bg(run_cmd.format(archive=download.temp_path))
			if proc.returncode != 0:
				raise FileIntegrityError(f"File {download.temp_path} downloaded from {download.request.url} does not appear to be a valid {arc_desc} archive!")
			log.info(f"Download from {download.request.url} verified as valid {arc_desc} archive.")
		return download

	async def fetch_completion_callback(self, download: Download) -> StoreObject:
		"""
		This method is intended to be called *once* when an actual in-progress download of a tarball (by
		the Spider) has completed. It performs several important finalization actions upon successful
		download:

		1. The downloaded file will be stored in the BLOS, and the resultant BLOSObject will be assigned to
		``response.blos_object``.

		2. The Spider will be told to clean up the temporary file, as it will not be accessed directly by
		   anyone -- only the permanent file inserted into the BLOS will be handed back (via
		   ``response.blos_object``.

		We pass this to any Download() object we instantiate so that it has proper post-actions defined
		for it.
		"""

		store_obj: StoreObject = self.blos.insert_download(download)
		if self.spider:
			self.spider.cleanup(download)
		return store_obj
