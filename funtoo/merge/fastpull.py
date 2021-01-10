#!/usr/bin/python3

import logging
import os
import random
import pymongo
from pymongo import MongoClient

hub = None


def __init__():
	mc = MongoClient()
	fp = hub.FASTPULL = mc.metatools.fastpull
	fp.create_index([("hashes.sha512", pymongo.ASCENDING), ("filename", pymongo.ASCENDING)], unique=True)
	fp.create_index([("rand_id", pymongo.ASCENDING)], unique=True)
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
	#  atom: atom
	# Some items may be omitted based on whether they are in our legacy DB or not.


def complete_artifact(artifact):
	"""
	Provided with an artifact and expected final data (hashes and size), we will attempt to locate the artifact
	binary data in the fastpull database. If we find it, we 'complete' the artifact so it is usable for extraction
	or looking at final hashes, with a correct on-disk path to where the data is located.

	Note that when we look for the completed artifact, we don't care if our data has a different 'name' -- as long
	as the binary data on disk has matching hashes and size.

	If not found, simply return None.

	This method was originally intended to allow us to specify expected final data, aka hashes, that we expect to
	see. But this is not really used by autogen at the moment. The reason is that while emerge and ebuild do
	Manifest/hash validation on the client side, this is because we want to ensure that what was downloaded by the
	client matches what was set by the server. But we don't have such checks on just the server side.
	"""
	fp = artifact.fastpull_path
	if not fp:
		return None
	hashes = hub.pkgtools.download.calc_hashes(fp)
	if hashes["sha512"] != artifact.final_data["sha512"]:
		return None
	if hashes["size"] != artifact.final_data["size"]:
		return None
	artifact.final_data = hashes
	artifact.final_path = fp
	return artifact


def create_fastpull_db_entry(artifact, rand_id=None):
	db_entry = {}
	db_entry["hashes"] = artifact.final_data
	db_entry["filename"] = artifact.final_name
	if rand_id:
		db_entry["rand_id"] = rand_id
	else:
		db_entry["rand_id"] = "".join(random.choice("abcdef0123456789") for _ in range(128))
	hub.FASTPULL.insert_one(db_entry)


async def inject_into_fastpull_db(artifact):
	"""
	We assume that we have a downloaded artifact. Then we attempt to add to our fastpull database.
	"""
	await artifact.ensure_completed()
	fastpull_path = artifact.fastpull_path
	if os.path.islink(fastpull_path):
		# This will fix-up the situation where we used symlinks in fastpull rather than copying the file. It will
		# replace the symlink with the actual file. I did this for quickly migrating the legacy fastpull db. Once
		# I have migrated it over, this condition can probably be safely removed.
		actual_file = os.path.realpath(fastpull_path)
		if os.path.exists(actual_file):
			os.unlink(fastpull_path)
			os.link(actual_file, fastpull_path)
	elif not os.path.exists(fastpull_path):
		try:
			os.makedirs(os.path.dirname(fastpull_path), exist_ok=True)
			os.link(artifact.final_path, fastpull_path)
		except Exception as e:
			# Multiple doits running in parallel, trying to link the same file -- could cause exceptions:
			logging.error(f"Exception encountered when trying to link into fastpull (may be harmless) -- {repr(e)}")
	create_fastpull_db_entry(artifact)
