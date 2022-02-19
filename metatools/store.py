import hashlib
import os
from collections import OrderedDict
from typing import Mapping

from bson import UuidRepresentation
from bson.codec_options import TypeRegistry
from bson.json_util import dumps, JSONOptions, loads

# Notes:
#
# So far, what I have for a FileStore works great, as long as the process/thread has exclusive access
# to the underlying data since there is no locking.
#
# To work around the locking issue, one possibility is to have every datastore fronted by a separate
# process or distinct, dedicated thread that implements a ZeroMQ protocol that others can connect to
# utilize the datastore. This would cause all requests to be processed linearly while still offering
# high performance and flexibility.
#
# Within a single process, subpop can be used to keep a global registry of running services.
#
# For merge-kits, this is more complicated because merge-kits currently 'fires off' multiple separate
# processes for each "doit" call, and these processes need to be informed of any globally-running
# fetch cache, BLOS, etc. However, this could easily be done with environment variables or command-
# line arguments. So this is a very doable solution. So this could work as follows:
#
# "doit", run standalone with no special command-line options, would try to acquire an exclusive lock
# to all stores it needs, and fail if they were in use.
#
# If "merge-kits" is run, it will acquire an exclusive lock to all the stores it and any "doit" commands
# would need. And when firing up "doit" subprocesses, it would specify via command-line argument or
# environment variables the sockets to be used to connect to these already-running stores.
#
# This model would not require locking at all, and just works by exclusively locking the resource but
# then delegating access to subprocesses by pointing them to where to connect.
#
# On one hand, this is 'more complicated' than 'just using mongo' -- however:
#
# 1. We still have a need for BLOB storage on the filesystem for fastpull, so we need some file storage
#    anyway; and
# 2. This extra complexity forces us to think about how we are accessing our data and helps to ensure that
#    we are avoiding race conditions and other problems that can come up when we just say "oh, it's all in
#    mongo" and don't worry about how these various processes and threads may interact negatively with
#    each other.
# 3. Apart from a bit of delegation logic, and a light ZeroMQ layer, this does provide a way to totally
#    avoid locking, have a fast storage engine with no need for mongo, have a solution that works for
#    developer-style 'doit' calls and prod-style 'merge-kits' calls -- so seems like a good solution :)
#
# For implementing a basic 'doit', exclusive access to the stores should be sufficient and this should
# just work. But we will immediately run into problems if there isn't another mechanism for access when
# we get to more advanced tools.
#
# Below:
# This is equivalent to CANONICAL_JSON_OPTIONS, but we use OrderedDicts for representing objects (good to
# ensure consistency when storing/retrieving dictionaries)

JSON_OPTIONS = JSONOptions(
			strict_number_long=True, datetime_representation=1, strict_uuid=True, json_mode=2,
			document_class=OrderedDict, tz_aware=False, uuid_representation=UuidRepresentation.UNSPECIFIED,
			unicode_decode_error_handler='strict', tzinfo=None,
			type_registry=TypeRegistry(type_codecs=[], fallback_encoder=None))



class NotFoundError(Exception):
	pass


def extract_data_by_keyspec(index_field, data):
	"""
	This method accepts a string like "foo.bar", and will traverse dict hierarchy ``metadata`` to retrieve the specified
	element. Each '.' represents a depth in the dictionary hierarchy.
	"""
	index_split = index_field.split(".")
	cur_data = data
	for index_part in index_split:
		if index_part not in cur_data:
			raise KeyError(f"Attempting to retrieve field {index_field}, but does not exist ({index_part})")
		elif not isinstance(cur_data, Mapping):
			raise KeyError(f"Attempting to retrieve field {index_field}, but did not find it in supplied data.")
		cur_data = cur_data[index_part]
	return cur_data


def expand_keyspec(keyspec):
	"""
	This function takes a mongo-style query string like::

	  { "pkginfo.cat" : "sys-apps", "pkginfo.pkg" : "portage" }

	...and will convert it to the actual dictionary we want to match, which would be::

	  { "pkginfo" : {
	    "cat" : "sys-apps",
	    "pkg" : "portage"
	  } }

	the store.read() and store.delete() methods take a KeySpec like the first example.
	"""
	out = {}
	for keyspec_atom, val in keyspec.items():
		keyspec_atom_split = keyspec_atom.split(".")
		cur_out = out
		for depth_atom in keyspec_atom_split[:-1]:
			if depth_atom not in cur_out:
				cur_out[depth_atom] = {}
			cur_out = cur_out[depth_atom]
		cur_out[keyspec_atom_split[-1]] = val
	return out


class KeySpecification:
	pass


class HashKeySpecification(KeySpecification):

	def __init__(self, key_spec):
		self.key_spec = key_spec

	def __repr__(self):
		return f"HashKey({self.key_spec}"

	def data_as_hash(self, data):
		return extract_data_by_keyspec(self.key_spec, data)

	def validate_specdict(self, spec_dict):
		if self.key_spec not in spec_dict:
			raise KeyError(f"Was expecting {self.key_spec} to be specified for query.")

	def validate_data(self, data):
		extract_data_by_keyspec(self.key_spec, data)

	def specdict_as_hash(self, spec_dict):
		self.validate_specdict(spec_dict)
		return spec_dict[self.key_spec]


class DerivedKeySpecification(KeySpecification):

	def __init__(self, key_spec_list):
		self.key_spec_list = key_spec_list

	def __repr__(self):
		return f"DerivedKeys({self.key_spec_list})"

	def data_as_hash(self, data):
		return hashlib.sha512(dumps(self.compound_value(data), json_options=JSON_OPTIONS, sort_keys=True).encode("utf-8")).hexdigest()

	def compound_value(self, data):
		value = OrderedDict()
		for key_spec in self.key_spec_list:
			index_data = extract_data_by_keyspec(key_spec, data)
			value[key_spec] = index_data
		return value

	def validate_data(self, data):
		for key_spec in self.key_spec_list:
			extract_data_by_keyspec(key_spec, data)

	def validate_specdict(self, spec_dict):
		expected_set = set(self.key_spec_list)
		provided_set = set(spec_dict.keys())
		unrecognized = provided_set - expected_set
		missing = expected_set - provided_set
		if unrecognized:
			raise KeyError(f"Unrecognized key specifications in query: {unrecognized}")
		if missing:
			raise KeyError(f"Missing key specifications in query: {missing}")

	def specdict_as_hash(self, spec_dict):
		self.validate_specdict(spec_dict)
		return self.data_as_hash(expand_keyspec(spec_dict))


class StorageBackend:
	store = None

	def create(self, store):
		self.store = store

	def write(self, data, blob_path=None):
		pass

	def read(self, spec_dict, get_blob=False):
		pass

	def delete(self, data):
		pass


class FileStorageBackend(StorageBackend):
	root = None

	def __init__(self, db_base_path):
		self.db_base_path = db_base_path

	def create(self, store):
		self.store = store
		self.root = os.path.join(self.db_base_path, self.store.collection)
		if self.store.prefix is not None:
			self.root = os.path.join(self.root, self.store.prefix)
		os.makedirs(self.root, exist_ok=True)

	def encode_data(self, data):
		# We sort the keys so we always have a consistent representation of dictionary keys on disk.
		return dumps(data, json_options=JSON_OPTIONS, sort_keys=True).encode('utf-8')

	def decode_data(self, path):
		with open(path, "rb") as f:
			in_string = f.read().decode("utf-8")
			return loads(in_string, json_options=JSON_OPTIONS)

	def write(self, data, blob_path=None):
		sha = self.store.key_spec.data_as_hash(data)
		dir_index = f"{sha[0:2]}/{sha[2:4]}/{sha[4:6]}"
		out_path = f"{self.root}/{dir_index}/{sha}"
		os.makedirs(os.path.dirname(out_path), exist_ok=True)
		self._write_phase2(out_path, data, blob_path)

	def _write_phase2(self, out_path, data, blob_path=None):
		os.makedirs(os.path.dirname(out_path), exist_ok=True)
		with open(out_path, 'wb') as f:
			f.write(self.encode_data(data))
		if blob_path:
			blob_outpath = out_path + ".blob"
			if os.path.exists(blob_outpath):
				os.unlink(blob_outpath)
			os.link(blob_path, blob_outpath)
			return blob_path

	def read(self, spec_dict, get_blob=False):
		sha = self.store.key_spec.specdict_as_hash(spec_dict)
		dir_index = f"{sha[0:2]}/{sha[2:4]}/{sha[4:6]}"
		in_path = f"{self.root}/{dir_index}/{sha}"
		if not os.path.exists(in_path):
			if not get_blob:
				return None
			else:
				return None, None
		if not get_blob:
			return self.decode_data(in_path)
		else:
			blob_path = in_path + ".blob"
			return self.decode_data(in_path), blob_path if os.path.exists(blob_path) else None

	def delete(self, spec_dict):
		sha = self.store.key_spec.specdict_as_hash(spec_dict)
		dir_index = f"{sha[0:2]}/{sha[2:4]}/{sha[4:6]}"
		in_path = f"{self.root}/{dir_index}/{sha}"
		if os.path.exists(in_path):
			os.unlink(in_path)
		blob_path = in_path + ".blob"
		if os.path.exists(blob_path):
			os.unlink(blob_path)


class Store:

	backend: StorageBackend = None
	collection = None
	prefix = None
	key_spec = None
	required_spec = None

	def __init__(self, collection=None, prefix=None, key_spec=None, required_spec=None, backend=None):
		if collection is not None:
			self.collection = collection
		if prefix is not None:
			self.prefix = prefix
		if key_spec is not None:
			self.key_spec = key_spec
		if required_spec is not None:
			self.required_spec = required_spec
		if backend is not None:
			self.backend = backend
		self.backend.create(self)

	def write(self, data, blob_path=None):
		if self.required_spec:
			self.required_spec.validate_data(data)
		return self.backend.write(data, blob_path=blob_path)

	def read(self, spec_dict: dict, get_blob=False):
		return self.backend.read(spec_dict, get_blob=get_blob)

	def delete(self, key_spec: dict):
		return self.backend.delete(key_spec)
