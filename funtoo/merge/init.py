import logging

from merge_utils.config import Configuration
import pymongo
from pymongo import MongoClient


def __init__(model, prod=None, push=False, release=None, **kwargs):
	# When kits are being regenerated, we will update these variables to contain counts of various kinds of
	# errors, so we can display a summary at the end of processing all kits. Users can consult the correct
	# logs in ~/repo_tmp/tmp/ for details.
	model.METADATA_ERROR_STATS = []
	model.PROCESSING_WARNING_STATS = []
	model.CURRENT_SOURCE_DEF = None
	model.SOURCE_REPOS = {}
	model.PUSH = push
	model.FDATA = None
	model.PROD = False
	if prod is True:
		model.PROD = prod
	logging.warning(f"PROD {getattr(model, 'PROD', 'NOT DEFINED')}")
	model.RELEASE = release
	# Passing "fastpull" kwarg to Configuration:
	model.MERGE_CONFIG = Configuration(prod=prod, **kwargs)
	mc = MongoClient()
	dd = model.DEEPDIVE = mc.metatools.deepdive
	dd.create_index("atom")
	dd.create_index([("kit", pymongo.ASCENDING), ("category", pymongo.ASCENDING), ("package", pymongo.ASCENDING)])
	dd.create_index("catpkg")
	dd.create_index("relations")
	dd.create_index("md5")
	dd.create_index("files.name", partialFilterExpression={"files": {"$exists": True}})

	di = model.DISTFILE_INTEGRITY = mc.metatools.distfile_integrity
	di.create_index([("category", pymongo.ASCENDING), ("package", pymongo.ASCENDING), ("distfile", pymongo.ASCENDING)])

	fp = model.FASTPULL = mc.metatools.fastpull
	fp.create_index([("hashes.sha512", pymongo.ASCENDING), ("filename", pymongo.ASCENDING)], unique=True)
	# rand_ids don't need to be unique -- they can be shared if they are pointing to the same underlying file.
	fp.create_index([("rand_id", pymongo.ASCENDING)])
	#
	# Structure of Fastpull database:
	#
	# filename: actual destination final_name, string.
	# hashes: dictionary containing:
	#   size: file size
	#   sha512: sha512 hash
	#   ... other hashes
	# rand_id: random_id from legacy fastpull. We are going to keep using this for all our new fastpulls too.
	# src_uri: URI file was downloaded from.
	# fetched_on: timestamp file was fetched on.
	# refs: list of references in packages, each item in list a dictionary in the following format:
	#  kit: kit
	#  catpkg: catpkg
	#  Some items may be omitted from the above list.

	model.THIRD_PARTY_MIRRORS = None
	model.MIRROR = False
	model.NEST_KITS = False
	model.CREATE_BRANCHES = True
