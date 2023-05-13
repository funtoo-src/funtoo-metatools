# The kit cache has been broken out into its own class so it is not so tightly integrated into merge-kits. This
# allows utilities to be written that can easily access kit-cache data that do not depend on the entire merge-kits
# workflow, such as a tool to scan a kit and see what distfiles are missing from the BLOS, for example.

import os
import json

from metatools.model import get_model

CACHE_DATA_VERSION = "1.0.6"

model = get_model("metatools")


class KitCache:
	json_data = None

	def __init__(self, release, name, branch):
		self.release = release
		self.name = name
		self.branch = branch
		self.writes = set()
		self.misses = set()
		self.retrieved_atoms = set()
		self.metadata_errors = {}
		self.processing_warnings = []

	def load(self):
		# Upgrade to new path format, keeping old kit-cache:
		if not os.path.exists(self.path):
			if os.path.exists(self.old_path):
				os.makedirs(os.path.dirname(self.path), exist_ok=True)
				os.link(self.old_path, self.path)
				os.unlink(self.old_path)

		# This is the regular load logic:
		if os.path.exists(self.path):
			self.json_data = self.load_json()
		else:
			self.json_data = {"atoms": {}}

	def load_json(self, validate=True):
		"""
		This is a stand-alone function for loading kit cache JSON data, in case someone like me wants to manually load
		it and look at it. It will check to make sure the CACHE_DATA_VERSION matches what this code is designed to
		inspect, by default.
		"""
		with open(self.path, "r") as f:
			try:
				kit_cache_data = json.loads(f.read())
			except json.decoder.JSONDecodeError as jde:
				model.log.error(f"Unable to parse JSON in {self.path}: {jde}")
				raise jde
			if validate:
				if "cache_data_version" not in kit_cache_data:
					model.log.error("JSON invalid or missing cache_data_version.")
					return None
				elif kit_cache_data["cache_data_version"] != CACHE_DATA_VERSION:
					model.log.error(
						f"Cache data version is {kit_cache_data['cache_data_version']} but needing {CACHE_DATA_VERSION}")
					return None
			return kit_cache_data

	@property
	def path(self):
		return os.path.join(model.temp_path, "kit_cache", self.release, f"{self.name}-{self.branch}")

	@property
	def old_path(self):
		return os.path.join(model.temp_path, "kit_cache", f"{self.name}-{self.branch}")

	def __setitem__(self, atom, value):
		self.json_data["atoms"][atom] = value
		self.writes.add(atom)

	def items(self):
		yield from self.json_data["atoms"].items()

	def get_atom(self, atom, md5, manifest_md5, merged_eclasses):
		"""
		Read from our in-memory kit metadata cache. Return something if available, else None.

		This will validate that our in-memory record has a matching md5 and that md5s of all
		eclasses match. AND the md5 of the Manifest (if any exists) matches.
		Otherwise, we treat this as a cache miss.

		This method is really designed to be used by the KitGenerator and not by any stand-alone
		tools, as it requires a ``merged_eclasses`` object to be passed to it which is used to
		validate that the cache item is current.
		"""
		existing = None
		if atom in self.json_data["atoms"]:
			if not self.json_data["atoms"][atom]:
				model.log.error(f"Kit cache atom {atom} invalid due to empty data")
				bad = True
			elif self[atom]["md5"] != md5:
				model.log.error(
					f"Kit cache atom {atom} ignored due to non-matching MD5 (if this recurs: non-deterministic ebuild?)")
				bad = True
			else:
				existing = self[atom]
				bad = False
				if "manifest_md5" not in existing:
					model.log.error(f"Kit cache atom {atom} ignored due to missing manifest md5 (incomplete? bug?)")
					bad = True
				elif manifest_md5 != existing["manifest_md5"]:
					model.log.error(
						f"Kit cache atom {atom} ignored due to non-matching manifest MD5 (if this recurs: may indicate bug.)")
					bad = True
				elif existing["eclasses"]:
					for eclass, md5 in existing["eclasses"]:
						if eclass not in merged_eclasses.hashes:
							model.log.warning(
								f"Kit cache atom {atom} can't be used due to missing eclass {eclass}.eclass")
							bad = True
							break
						if merged_eclasses.hashes[eclass] != md5:
							model.log.warning(
								f"Kit cache atom {atom} can't be used due to changed MD5 for {eclass}.eclass")
							bad = True
							break
			if bad:
				# stale cache entry, don't use.
				existing = None
		return existing

	def __getitem__(self, item):
		return self.json_data["atoms"][item]

	def keys(self):
		return self.json_data["atoms"].keys()

	def save(self, prune=True):
		remove_keys = set()
		if prune:
			all_keys = set(self.json_data["atoms"].keys())
			remove_keys = all_keys - (self.retrieved_atoms | self.writes)
			extra_atoms = self.retrieved_atoms - all_keys
			for key in remove_keys:
				del self.json_data["atoms"][key]
			if len(extra_atoms):
				model.log.error("THERE ARE EXTRA ATOMS THAT WERE RETRIEVED BUT NOT IN CACHE!")
				model.log.error(f"{extra_atoms}")
		outdata = {
			"cache_data_version": CACHE_DATA_VERSION,
			"atoms": self.json_data["atoms"],
			"metadata_errors": self.metadata_errors,
		}
		if len(self.metadata_errors):
			log_out = model.log.warning
		else:
			log_out = model.log.debug
		log_out(f"Flushed {self.name}. {len(self.json_data['atoms'])} atoms. Removed {len(remove_keys)} keys. {len(self.metadata_errors)} errors.")
		os.makedirs(os.path.dirname(self.path), exist_ok=True)
		with open(self.path, "w") as f:
			f.write(json.dumps(outdata))
		error_outpath = os.path.join(
			model.temp_path, f"metadata-errors-{self.name}-{self.branch}.log"
		)
		if len(self.metadata_errors):
			model.metadata_error_stats.append(
				{"name": self.name, "branch": self.branch, "count": len(self.metadata_errors)}
			)
			with open(error_outpath, "w") as f:
				f.write(json.dumps(self.metadata_errors))
		else:
			if os.path.exists(error_outpath):
				os.unlink(error_outpath)

		error_outpath = os.path.join(model.temp_path, f"warnings-{self.name}-{self.branch}.log")
		if len(self.processing_warnings):
			model.processing_warning_stats.append(
				{"name": self.name, "branch": self.branch, "count": len(self.processing_warnings)}
			)
			with open(error_outpath, "w") as f:
				f.write(json.dumps(self.processing_warnings))
		else:
			if os.path.exists(error_outpath):
				os.unlink(error_outpath)
