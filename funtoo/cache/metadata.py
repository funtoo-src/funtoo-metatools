#!/usr/bin/env python3

import json
import logging
import os
import sys

# Increment this constant whenever we update the kit-cache to store new data. If what we retrieve is an earlier
# version, we'll consider the kit cache stale and regenerate it.

CACHE_DATA_VERSION = "1.0.4"


def get_outpath(hub, repo_obj):
	os.makedirs(os.path.join(hub.MERGE_CONFIG.temp_path, "kit_cache"), exist_ok=True)
	return os.path.join(hub.MERGE_CONFIG.temp_path, "kit_cache", f"{repo_obj.name}-{repo_obj.branch}")


def fetch_kit(hub, repo_obj):
	"""
	Grab cached metadata for an entire kit from serialized JSON, with a single query.
	"""
	outpath = hub._.get_outpath(repo_obj)
	if os.path.exists(outpath):
		with open(outpath, "r") as f:
			kit_cache_data = json.loads(f.read())
			if "cache_data_version" not in kit_cache_data or kit_cache_data["cache_data_version"] != CACHE_DATA_VERSION:
				atoms = {}
			else:
				atoms = kit_cache_data["atoms"]
	else:
		atoms = {}
	repo_obj.KIT_CACHE = atoms

	# Because these variables are written to by multiple threads, we can't really have threads adding stuff
	# without locking I don't think....

	repo_obj.KIT_CACHE_RETRIEVED_ATOMS = set()
	repo_obj.KIT_CACHE_MISSES = set()
	repo_obj.KIT_CACHE_WRITES = set()


def flush_kit(hub, repo_obj, save=True, prune=True):
	"""
	Write out our in-memory copy of our entire kit metadata, which may contain updates.

	If `save` is False, simply empty without saving.

	If no changes have been made to the kit cache, no changes need to be saved.

	If there were changes, and if `prune` is True, any unaccessed (unread) item will be removed from the cache.
	This is intended to clean out stale entries during tree regeneration.
	"""
	if prune:
		num_pruned = 0
		# anything that was not accessed, remove from cache.

		logging.info(f"{len(repo_obj.KIT_CACHE.keys())} items are in the kit cache.")
		logging.info(
			f"There have been {len(repo_obj.KIT_CACHE_RETRIEVED_ATOMS)} atoms read, {len(repo_obj.KIT_CACHE_MISSES)} cache misses and {len(repo_obj.KIT_CACHE_WRITES)} updates to items."
		)
		all_keys = set(repo_obj.KIT_CACHE.keys())
		remove_keys = all_keys - (repo_obj.KIT_CACHE_RETRIEVED_ATOMS | repo_obj.KIT_CACHE_WRITES)
		extra_atoms = repo_obj.KIT_CACHE_RETRIEVED_ATOMS - all_keys
		for key in remove_keys:
			del repo_obj.KIT_CACHE[key]
		if len(extra_atoms):
			logging.error("THERE ARE EXTRA ATOMS THAT WERE RETRIEVED BUT NOT IN CACHE!")
			logging.error(f"{extra_atoms}")
	if save:
		outpath = hub._.get_outpath(repo_obj)
		outdata = {"cache_data_version": CACHE_DATA_VERSION, "atoms": repo_obj.KIT_CACHE}
		with open(outpath, "w") as f:
			f.write(json.dumps(outdata))


def get_atom(hub, repo_obj, atom, md5, manifest_md5, eclass_hashes):
	"""
	Read from our in-memory kit metadata cache. Return something if available, else None.

	This will validate that our in-memory record has a matching md5 and that md5s of all
	eclasses match. AND the md5 of the Manifest (if any exists) matches.
	Otherwise we treat this as a cache miss.
	"""
	existing = None
	if atom in repo_obj.KIT_CACHE and repo_obj.KIT_CACHE[atom]["md5"] == md5:
		existing = repo_obj.KIT_CACHE[atom]
		bad = False
		if "manifest_md5" not in existing:
			bad = True
		elif manifest_md5 != existing["manifest_md5"]:
			bad = True
		elif existing["eclasses"]:
			for eclass, md5 in existing["eclasses"]:
				if eclass not in eclass_hashes:
					bad = True
					break
				if eclass_hashes[eclass] != md5:
					bad = True
					break
		if bad:
			# stale cache entry, don't use.
			existing = None
	return existing


def update_atom(hub, repo_obj, td_out):
	"""
	Update our in-memory record for a specific ebuild atom on disk that has changed. This will
	be written out by flush_kit(). Right now we just record it in memory.

	"""
	repo_obj.KIT_CACHE[td_out["atom"]] = td_out
	repo_obj.KIT_CACHE_WRITES.add(td_out["atom"])
