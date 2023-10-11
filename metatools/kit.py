#!/usr/bin/env python3
import asyncio
import glob
import json
import os
import sys
import threading
from collections import defaultdict
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import cpu_count
from typing import Union

from metatools.kit_cache import KitCache
from subpop.util import AttrDict

import metatools.steps
from metatools.release import SourcedKit, AutoGeneratedKit
from metatools.hashutils import get_md5
from metatools.metadata import AUXDB_LINES, get_catpkg_relations_from_depstring, get_filedata, extract_ebuild_metadata, strip_rev
from metatools.model import get_model
from metatools.tree import GitTreeError, Tree
from metatools.cmd import run_shell
from metatools.zmq.app_core import RouterListener

model = get_model("metatools")


class EclassHashCollection:
	"""
	This is just a simple class for storing the path where we grabbed all the eclasses from plus
	the mapping from eclass name (ie. 'eutils') to the hexdigest of the generated hash.

	You can add two collections together, with the last collection's eclasses taking precedence
	over the first. The concept is to be able to this::

	  all_eclasses = core_kit_eclasses + llvm_eclasses + this_kits_eclasses
	"""

	def __init__(self, path=None, paths=None, hashes=None):
		if paths:
			self.paths = paths
		else:
			self.paths = []
		if hashes:
			self.hashes = hashes
		else:
			self.hashes = {}
		if path and (hashes or paths):
			raise AttributeError("Don't use path= with hashes= or paths= -- pick one.")
		if path:
			self.add_path(path)

	def add_path(self, path, scan=True):
		"""
		Adds a path to self.paths which will take precedence over any existing paths.
		"""
		self.paths = [path] + self.paths
		if scan:
			self.scan_path(os.path.join(path, "eclass"))

	def __add__(self, other):
		paths = self.paths + other.paths
		hashes = self.hashes.copy()
		hashes.update(other.hashes)
		new_obj = self.__class__(paths=paths, hashes=hashes)
		model.log.debug(
			f"EclassHashCollection: Adding {len(other.hashes.keys())} and {len(self.hashes.keys())} -- now have {len(new_obj.hashes.keys())}")
		return new_obj

	def scan_path(self, eclass_scan_path):
		scan_count = 0
		if os.path.isdir(eclass_scan_path):
			for eclass in os.listdir(eclass_scan_path):
				if not eclass.endswith(".eclass"):
					continue
				eclass_path = os.path.join(eclass_scan_path, eclass)
				eclass_name = eclass[:-7]
				self.hashes[eclass_name] = get_md5(eclass_path)
				scan_count += 1
		model.log.debug(f"EclassHashCollection: Found {scan_count} eclasses in path {eclass_scan_path}.")


class SimpleKitGenerator:
	"""Our steps-based workflow requires a KitGenerator. If we want to use Steps, but don't have kits we're
	working with, we can instead use this minimal implementation (used by bin/merge-gentoo-staging.)"""
	def __init__(self, out_tree):
		self.out_tree = out_tree


class KitGenerator:
	"""
	This class represents the work associated with generating a Kit. A ``Kit`` (defined in metatools/files/release.py)
	is passed to the constructor of this object to define settings, and stored within this object as ``self.kit``.

	The KitGenerator takes care of creating or connecting to an existing Git tree that is used to house the results of
	the kit generation, and this Git tree object is stored at ``self.out_tree``.

	The ``self.generate()`` method (and supporting methods) take care of regenerating the Kit. Upon completion,
	``self.kit_sha1`` is set to the SHA1 of the commit containing these updates.
	"""

	kit_sha1 = None
	out_tree = None
	active_repos = set()

	kit_cache = None

	eclasses = None
	merged_eclasses = None
	is_master = None
	initialized = False

	def __repr__(self):
		return f"KitGenerator(kit.name={self.kit.name}, kit.branch={self.kit.branch}, kit.kind={self.kit.__class__.__name__})"

	def __init__(self, controller, kit: Union[SourcedKit, AutoGeneratedKit], is_master=False):
		self.controller = controller
		self.kit = kit
		self.is_master = is_master

		kit_config = model.release_yaml.get_repo_config(self.kit.name)

		if model.nest_kits:
			root = os.path.join(model.dest_trees, "meta-repo/kits", kit.name)
		else:
			root = os.path.join(model.dest_trees, kit.name)
		self.out_tree = model.git_class(
			name=kit.name,
			branch=kit.branch,
			url=kit_config['url'] if model.prod else None,
			root=root,
			origin_check=True if model.prod else None,
			mirrors=kit_config['mirrors'],
			create_branches=model.create_branches,
			model=model,
			**model.git_kwargs
		)
		self.kit_cache = KitCache(model.release, name=kit.name, branch=kit.branch)

	async def initialize(self):
		await self.out_tree.initialize()
		# load on-disk JSON metadata cache into memory:

		self.kit_cache.load()
		self.initialized = True

	async def run(self, steps):
		"""
		This command runs a series of steps. What I need to add is proper propagation of errors to caller.
		"""
		for step in steps:
			if step is not None:
				model.log.info(f"Running step {step.__class__.__name__} for {self.out_tree.root}")
				try:
					await step.run(self)
				except Exception as e:
					model.log.critical(f"Step {step.__class__.__name__} failed with Exception: {e}")
					raise e

	def iter_ebuilds(self):
		"""
		This function is a generator that scans the specified path for ebuilds and yields all
		the ebuilds it finds in this kit. Used for metadata generation.
		"""

		for catdir in os.listdir(self.out_tree.root):
			catpath = os.path.join(self.out_tree.root, catdir)
			if not os.path.isdir(catpath):
				continue
			for pkgdir in os.listdir(catpath):
				pkgpath = os.path.join(catpath, pkgdir)
				if not os.path.isdir(pkgpath):
					continue
				for ebfile in os.listdir(pkgpath):
					if ebfile.endswith(".ebuild"):
						yield os.path.join(pkgpath, ebfile)

	def gen_ebuild_metadata(self, atom, merged_eclasses, ebuild_path):
		self.kit_cache.misses.add(atom)

		env = {}
		env["PF"] = os.path.basename(ebuild_path)[:-7]
		env["CATEGORY"] = ebuild_path.split("/")[-3]
		pkg_only = ebuild_path.split("/")[-2]  # JUST the pkg name "foobar"
		reduced, rev = strip_rev(env["PF"])
		if rev is None:
			env["PR"] = "r0"
			pkg_and_ver = env["PF"]
		else:
			env["PR"] = f"r{rev}"
			pkg_and_ver = reduced
		env["P"] = pkg_and_ver
		env["PV"] = pkg_and_ver[len(pkg_only) + 1:]
		env["PN"] = pkg_only
		env["PVR"] = env["PF"][len(env["PN"]) + 1:]

		infos = extract_ebuild_metadata(self, atom, ebuild_path, env, reversed(merged_eclasses.paths))

		if not isinstance(infos, dict):
			# metadata extract failure
			return None, None
		return env, infos

	def write_repo_cache_entry(self, atom, metadata_out):
		# if we successfully extracted metadata and we are told to write cache, write the cache entry:
		metadata_outpath = os.path.join(self.out_tree.root, "metadata/md5-cache")
		final_md5_outpath = os.path.join(metadata_outpath, atom)
		os.makedirs(os.path.dirname(final_md5_outpath), exist_ok=True)
		with open(os.path.join(metadata_outpath, atom), "w") as f:
			f.write(metadata_out)

	def license_extract(self, infos):
		if not infos:
			return set()
		elif "LICENSE" not in infos:
			return set()
		else:
			prelim = set(infos["LICENSE"].split()) - {'||', ')', '('}
			return {i for i in prelim if not i.endswith('?')}

	def get_ebuild_metadata(self, merged_eclasses, ebuild_path) -> set:
		"""
		This function will grab metadata from a single ebuild pointed to by `ebuild_path` and
		return it as a dictionary.

		This function sets up a clean environment and spawns a bash process which runs `ebuild.sh`,
		which is a file from Portage that processes the ebuild and eclasses and outputs the metadata
		so we can grab it. We do a lot of the environment setup inline in this function for clarity
		(helping the reader understand the process) and also to avoid bunches of function calls.
		"""

		basespl = ebuild_path.split("/")
		atom = basespl[-3] + "/" + basespl[-1][:-7]
		ebuild_md5 = get_md5(ebuild_path)
		cp_dir = ebuild_path[: ebuild_path.rfind("/")]
		manifest_path = cp_dir + "/Manifest"

		if not os.path.exists(manifest_path):
			manifest_md5 = None
		else:
			# TODO: this is a potential area of performance improvement. Multiple ebuilds in a single catpkg
			#		directory will result in get_md5() being called on the same Manifest file multiple times
			#		during a run. Cache might be good here.
			manifest_md5 = get_md5(manifest_path)

		# Try to see if we already have this metadata in our kit metadata cache.
		existing = self.kit_cache.get_atom(atom, ebuild_md5, manifest_md5, merged_eclasses)

		if existing:
			self.kit_cache.retrieved_atoms.add(atom)
			infos = existing["metadata"]
			self.write_repo_cache_entry(atom, existing["metadata_out"])
			return self.license_extract(infos)
		# TODO: Note - this may be a 'dud' existing entry where there was a metadata failure previously.
		else:
			env, infos = self.gen_ebuild_metadata(atom, merged_eclasses, ebuild_path)
			if infos is None:
				self.kit_cache[atom] = {}
				return set()

		eclass_out = ""
		eclass_tuples = []

		if infos["INHERITED"]:
			# Do common pre-processing for eclasses:
			for eclass_name in sorted(infos["INHERITED"].split()):

				if eclass_name not in merged_eclasses.hashes:
					errmsg = f"{atom}: can't find eclass hash for {eclass_name} -- {merged_eclasses.hashes}"
					model.log.error(errmsg)
					raise KeyError(errmsg)
				try:
					eclass_out += f"\t{eclass_name}\t{merged_eclasses.hashes[eclass_name]}"
					eclass_tuples.append((eclass_name, merged_eclasses.hashes[eclass_name]))
				except KeyError as ke:
					errmsg = f"{atom}: can't find eclass hash for {eclass_name} (2) -- {merged_eclasses.hashes}"
					model.log.error(errmsg)
					raise KeyError(errmsg)
		metadata_out = ""

		for key in AUXDB_LINES:
			if infos[key] != "":
				metadata_out += key + "=" + infos[key] + "\n"
		if len(eclass_out):
			metadata_out += "_eclasses_=" + eclass_out[1:] + "\n"
		metadata_out += "_md5_=" + ebuild_md5 + "\n"

		# Extended metadata calculation:

		td_out = {}
		relations = defaultdict(set)

		for key in ["DEPEND", "RDEPEND", "PDEPEND", "BDEPEND", "HDEPEND"]:
			if infos[key]:
				relations[key] = get_catpkg_relations_from_depstring(infos[key])
		all_relations = set()
		relations_by_kind = dict()

		for key, relset in relations.items():
			all_relations = all_relations | relset
			relations_by_kind[key] = sorted(list(relset))

		td_out["relations"] = sorted(list(all_relations))
		td_out["relations_by_kind"] = relations_by_kind
		td_out["category"] = env["CATEGORY"]
		td_out["revision"] = env["PR"].lstrip("r")
		td_out["package"] = env["PN"]
		td_out["catpkg"] = env["CATEGORY"] + "/" + env["PN"]
		td_out["atom"] = atom
		td_out["eclasses"] = eclass_tuples
		td_out["kit"] = self.out_tree.name
		td_out["branch"] = self.out_tree.branch
		td_out["metadata"] = infos
		td_out["md5"] = ebuild_md5
		td_out["metadata_out"] = metadata_out
		td_out["manifest_md5"] = manifest_md5
		if manifest_md5 is not None and "SRC_URI" in infos:
			td_out["files"] = get_filedata(infos["SRC_URI"], manifest_path)
		self.kit_cache[atom] = td_out
		self.write_repo_cache_entry(atom, metadata_out)
		return self.license_extract(infos)

	def gen_cache(self):
		"""
		Generate md5-cache metadata from a bunch of ebuilds, for this kit. Use a ThreadPoolExecutor to run as many threads
		of this as we have logical cores on the system.
		"""

		total_count_lock = threading.Lock()
		total_count = 0
		all_licenses = set()

		with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
			count = 0
			futures = []
			fut_map = {}

			for ebpath in self.iter_ebuilds():
				future = executor.submit(
					self.get_ebuild_metadata,
					self.merged_eclasses,
					ebpath
				)
				fut_map[future] = ebpath
				futures.append(future)

			for future in as_completed(futures):
				count += 1
				data = future.result()
				if data is None:
					sys.stdout.write("!")
				else:
					all_licenses |= data
					sys.stdout.write(".")
				sys.stdout.flush()

			with total_count_lock:
				total_count += count

		if total_count:
			model.log.info(f"Metadata for {total_count} ebuilds processed.")
		else:
			model.log.warning(f"No ebuilds were found when processing metadata.")
		return all_licenses

	async def fail(self):
		raise GitTreeError()

	async def initialize_sources(self):
		await self.kit.initialize_sources()

	async def generate_sourced(self):
		"""
		This function contains the full steps used for generating a "sourced" kit. These steps are:

		1. Run autogen in the sourced tree.
		2. Copy everything over from the sourced tree.

		Note that kit-fixups is not used in this case -- all autogens, ebuilds, eclasses, etc. come from the sourced tree.

		Once these steps are all done, the kit is ready for finalization (gencache, etc) and a git commit which will contain
		the new changes.
		"""

		src_tree = self.kit.source.tree
		await self.run([
			metatools.steps.Autogen(src_tree),
			metatools.steps.SyncFromTree(src_tree, exclude=["/profiles/repo_name", "/profiles/categories", "/metadata/**"], delete=False)
		])

	async def generate_autogenerated(self):
		"""
		This function produces steps to recreate the contents of an autogenerated kit. This is typically run with a
		destination kit that has been "emptied" and is ready to be regenerated from scratch:

		1. First, look at ``packages.yaml`` and copy over any specified eclasses and files from source repositories.
		2. Next, look at ``packages.yaml``, and copy over any specified ebuilds from any source repositories. Note
		   that we do not run autogen for source repositories used in this way.
		3. Next, *remove* any files we specifically want to exclude from the destination kit.

		In the second phase, we then perform the following actions:

		4. Run autogen on the proper part of kit-fixups.
		5. Copy over all resultant ebuilds, eclasses, licenses, etc from kit-fixups that should be copied.

		This ensures that kit-fixups overrides whatever was in the source repositories. Once these steps are all done,
		the kit is ready for finalization (gencache, etc.) and a git commit which will contain the new changes.
		"""

		await self.run(self.copy_eclasses_steps())
		await self.run(self.packages_yaml_copy_ebuilds_steps())
		await self.run([metatools.steps.RemoveFiles(self.kit.get_excludes())])

		await self.out_tree.git_add()

		await self.run(self.autogen_and_copy_from_kit_fixups())

	async def copy_licenses(self, used_licenses=None):
		needed_licenses = set()
		os.makedirs(f"{self.out_tree.root}/licenses", exist_ok=True)

		for license in used_licenses:
			if not os.path.exists(f"{self.out_tree.root}/licenses/{license}"):
				needed_licenses.add(license)

		for license in needed_licenses:
			found = self.kit.source.find_license(license)
			if found:
				await run_shell(f"cp {found} {self.out_tree.root}/licenses", logger=model.log)

	async def distfile_scan(self):
		self.kit_cache.load()
		# We can now perform the steps in distfile-kit-fetch as we have access to the kit-cache.
		# TODO: add code here
		# TODO: add extra code here to log any issues. We should temp. hook into the log handler
		#		to re-route any messages to an appropriate log file. This would actually be a good
		#		general idea as we don't have this mechanism in the autogen process right now
		#		(separated logs don't go to disk.) So maybe we want to implement this outside this
		#		method, in the KitJob.

	async def generate(self):
		"""
		This function contains the full step-flow for updating a kit. This function handles both autogenerated kits
		and sourced kits.

		Here is a basic overview of the process:

		1. The to-be-updated kit is completely emptied of all files. (``CleanTree()``)
		2. The basic metadata is created inside the kit to make it a valid, but empty overlay. (``GenerateRepoMetadata()``)
		3. Depending on what type of kit it is -- autogenerated or sourced -- the steps will be executed to populate the kit
		   with its updated contents.
		4. Various miscellaneous tasks will be executed -- creating a global licensing information file, cleaning up of Manifests, etc.
		5. The Portage metadata cache will be updated and stored inside the kit.
		6. Auto-generation of Python USE settings will be performed. This optimizes the Python USE experience for Funtoo users.
		7. Licenses used by the ebuilds will be copied over to the ``licenses/`` directory.
		7. A new git commit within the kit will be created based on the result of these steps.
		8. The HEAD SHA1 will be recorded so that we can record it later within the meta-repo metadata.
		"""
		if not self.initialized:
			await self.initialize()

		await self.run([
			metatools.steps.CleanTree(),
			metatools.steps.GenerateRepoMetadata(self.kit.name, aliases=self.kit.aliases, masters=self.kit.masters, priority=self.kit.priority),
			metatools.steps.SyncFiles(model.kit_fixups.root, {"LICENSE.txt": "LICENSE.txt"}),
		])

		if isinstance(self.kit, AutoGeneratedKit):
			await self.generate_autogenerated()
		elif isinstance(self.kit, SourcedKit):
			await self.generate_sourced()

		##############################################################################
		# Now, we can run any post-steps to get the tree in ready-to-commit condition:
		##############################################################################

		await self.run([
			metatools.steps.FindAndRemove(["__pycache__"]),
			metatools.steps.FindAndRemove(["COPYRIGHT.txt"]),  # replaced with COPYRIGHT.rst
			metatools.steps.GenerateLicensingFile(text=self.kit.get_copyright_rst()),
			metatools.steps.Minify(),

			metatools.steps.CreateCategories(),
		])

		if self.kit.name == "core-kit":
			await self.run([
				metatools.steps.ELTSymlinkWorkaround(),
				metatools.steps.ThirdPartyMirrors()
			])

		############################################################################################################
		# Now that all eclasses should be copied over, scan what we have:
		############################################################################################################

		# Generate 'merged eclasses', which is essentially all the eclasses from masters and the local kit 'smooshed'
		# into the complete set of eclasses available to the kit. This is used for metadata generation:

		self.eclasses = EclassHashCollection(path=self.out_tree.root)
		self.merged_eclasses = EclassHashCollection()

		for master in self.kit.masters:
			self.merged_eclasses += self.controller.master_jobs[master].eclasses
		self.merged_eclasses += self.eclasses

		############################################################################################################
		# Use lots of CPU (potentially) to generate/update metadata cache:
		############################################################################################################

		used_licenses = self.gen_cache()
		await self.copy_licenses(used_licenses=used_licenses)

		############################################################################################################
		# Python USE settings auto-generation and other finalization steps:
		############################################################################################################

		# We can now run all the steps that require access to metadata:

		# Funtoo and metatools has a feature where we will look at the configured Python kits for the release,
		# and auto-generate optimal Python USE settings for each kit in the release. This ensures that things
		# can be easily merged without weird Python USE errors. These settings are stored in the following
		# location in each kit in the release::
		#
		#	profiles/funtoo/kits/python-kit/<python-kit-branch>
		#
		# When 'ego sync' runs, it will ensure that these settings are automatically enabled based upon what
		# your currently-active python-kit is. This means that even if you have multiple python-kit branches
		# defined in your release, switching between them is seamless and Python USE settings for all packages
		# in the repository will auto-adapt to whatever Python kit is currently enabled.

		await self.run([metatools.steps.GenPythonUse()])
		update_msg = "Autogenerated tree updates."
		await self.out_tree.git_commit(message=update_msg, push=model.push)

		# save in-memory metadata cache to JSON:
		self.kit_cache.save()
		self.kit_sha1 = self.out_tree.head()
		# This will get passed as the "result" if run in a ThreadPoolGenerator() (when we call get_result())
		return self

	def copy_eclasses_steps(self):

		kit_copy_info = self.kit.eclass_include_info()
		mask = kit_copy_info["mask"]
		file_mask = map(lambda x: f"{x}.eclass", list(mask))
		my_steps = []
		for srepo_name, eclass_name_list in kit_copy_info["include"].items():
			copy_eclasses = set()
			for eclass_item in eclass_name_list:
				if eclass_item == "*":
					my_steps.append(metatools.steps.SyncDir(self.kit.source.repositories[srepo_name].tree, "eclass",
															exclude=file_mask))
				else:
					if eclass_item not in mask:
						copy_eclasses.add(eclass_item)
					else:
						model.log.warn(
							f"For kit {self.kit.name}, {eclass_item} is both included and excluded in the release YAML.")
			if copy_eclasses:
				copy_tuples = []
				for item in copy_eclasses:
					if item.split("/")[-1] not in mask:
						file_path = f"eclass/{item}.eclass"
						copy_tuples.append((file_path, file_path))
				my_steps.append(metatools.steps.CopyFiles(self.kit.source.repositories[srepo_name].tree, copy_tuples))
		return my_steps

	def get_source_repo(self):
		raise NotImplementedError()

	def packages_yaml_copy_ebuilds_steps(self):
		"""
		This method returns all steps related to the 'packages' entries in the package.yaml file, and getting these
		packages copied over from the source repositories. Note that we do not run autogen for any trees for which
		we are using in this way.
		"""
		my_steps = []
		# Copy over catpkgs listed in 'packages' section:
		for repo_name, packages in self.kit.get_kit_packages():
			self.active_repos.add(repo_name)
			my_steps += [
				metatools.steps.InsertEbuilds(self.kit.source.repositories[repo_name].tree, skip=None, replace=True,
											  move_maps=None, select=packages)]
		return my_steps

	def autogen_and_copy_from_kit_fixups(self):
		"""
		Return steps that will, as a whole, copy over everything from kit-fixups to a destination kit. These steps will include:

		1. Running autogen in the appropriate subdirectories inside kit-fixups.
		2. Copying over ebuilds from these subdirectories.

		The steps are ordered correctly so that "curated", "next", "1.4-release" directories have the proper precedence over one
		another (with a more specific release's ebuild overriding what might be in "curated".)

		The end result will be that opy over eclasses, licenses, profile info, and ebuild/eclass fixups from the kit-fixups repository.

		How the Algorithm Works
		=======================

		First, we are going to process the kit-fixups repository and look for ebuilds and eclasses to replace. Eclasses can be
		overridden by using the following paths inside kit-fixups:

		* kit-fixups/eclass/1.2-release <--------- global eclasses, get installed to all kits unconditionally for release (overrides those above)
		* kit-fixups/<kit>/global/eclass <-------- global eclasses for a particular kit, goes in all branches (overrides those above)
		* kit-fixups/<kit>/global/profiles <------ global profile info for a particular kit, goes in all branches (overrides those above)
		* kit-fixups/<kit>/<branch>/eclass <------ eclasses to install in just a specific branch of a specific kit (overrides those above)
		* kit-fixups/<kit>/<branch>/profiles <---- profile info to install in just a specific branch of a specific kit (overrides those above)

		Note that profile repo_name and categories files are excluded from any copying.

		Ebuilds can be installed to kits by putting them in the following location(s):

		* kit-fixups/<kit>/global/cat/pkg <------- install cat/pkg into all branches of a particular kit
		* kit-fixups/<kit>/<branch>/cat/pkg <----- install cat/pkg into a particular branch of a kit
		"""
		steps = []
		# Here is the core logic that copies all the fix-ups from kit-fixups (eclasses and ebuilds) into place:
		eclass_release_path = "eclass/%s" % model.release
		if os.path.exists(os.path.join(model.kit_fixups.root, eclass_release_path)):
			steps += [metatools.steps.SyncDir(model.kit_fixups.root, eclass_release_path, "eclass")]
		fixup_dirs = ["global", "curated", self.kit.branch]
		for fixup_dir in fixup_dirs:
			fixup_path = self.kit.name + "/" + fixup_dir
			if os.path.exists(model.kit_fixups.root + "/" + fixup_path):
				if os.path.exists(model.kit_fixups.root + "/" + fixup_path + "/eclass"):
					steps += [
						metatools.steps.InsertFilesFromSubdir(
							model.kit_fixups, "eclass", ".eclass", select="all", skip=None, src_offset=fixup_path
						)
					]
				if os.path.exists(model.kit_fixups.root + "/" + fixup_path + "/licenses"):
					steps += [
						metatools.steps.InsertFilesFromSubdir(
							model.kit_fixups, "licenses", None, select="all", skip=None, src_offset=fixup_path
						)
					]
				if os.path.exists(model.kit_fixups.root + "/" + fixup_path + "/profiles"):
					steps += [
						metatools.steps.InsertFilesFromSubdir(
							model.kit_fixups, "profiles", None, select="all", skip=["repo_name", "categories"],
							src_offset=fixup_path
						)
					]
				# copy appropriate kit readme into place:
				readme_path = fixup_path + "/README.rst"
				if os.path.exists(model.kit_fixups.root + "/" + readme_path):
					steps += [metatools.steps.SyncFiles(model.kit_fixups.root, {readme_path: "README.rst"})]

				# We now add a step to insert the fixups, and we want to record them as being copied so successive kits
				# don't get this particular catpkg. Assume we may not have all these catpkgs listed in our package-set
				# file...

				# TODO: since we are running autogen in a for-loop, below, there is always the possibility of parallelizing this code further.
				#		The only challenge is that we may be autogenning similar ebuilds, and thus we may be writing to the same Store() behind-the-scenes.

				steps += [
					metatools.steps.Autogen(model.kit_fixups, ebuildloc=fixup_path),
					metatools.steps.InsertEbuilds(model.kit_fixups, ebuildloc=fixup_path, select="all", skip=None,
												  replace=True)
				]
		return steps


class KitExecutionPool:

	def __init__(self, jobs, method="generate"):
		self.jobs = jobs
		self.method = method

	async def run(self):
		for kit_job in self.jobs:
			model.log.debug(f"KitExecutionPool: running job {kit_job}")
			await kit_job.initialize_sources()
			method = getattr(kit_job, self.method)
			try:
				await method()
				model.log.debug(f"KitExecutionPool: job {kit_job} complete")
			except Exception as e:
				model.log.exception("Kit job failure:")
				return False
		return True


class MoonBeam(RouterListener):

	def setup(self):
		if model.howdy:
			asyncio.create_task(self.howdy())

	async def howdy(self):
		while True:
			print("HOWDY")
			await asyncio.sleep(0.1)


class MetaRepoJobController:
	"""
	This class is designed to run the full meta-repo and kit regeneration process -- in other words, the entire
	technical flow of 'merge-kits' when it creates or updates kits and meta-repo. It is designed to "go through"
	all the kits in a release.
	"""

	master_jobs = {}
	kit_jobs = []
	model = None
	meta_repo = None
	# Does this job controller update meta-repo? If so, this get set to True, otherwise False.
	write = False
	moonbeam = None
	moonbeam_task = None

	def __init__(self, model, write=None):
		self.model = model
		self.moonbeam = MoonBeam("merge-kits", bind_addr=f"ipc://{self.model.moonbeam_socket}")
		if write:
			self.write = write
		assert isinstance(self.write, bool)

	def cleanup_error_logs(self):
		# This should be explicitly called at the beginning of every command that generates metadata for kits:

		for file in glob.glob(os.path.join(model.temp_path, "metadata-errors*.log")):
			os.unlink(file)

	def display_error_summary(self):
		model.log.debug("display_error_summary start")
		for stat_list, name, shortname in [
			(model.metadata_error_stats, "metadata extraction errors", "errors"),
			(model.processing_warning_stats, "warnings", "warnings"),
		]:
			if len(stat_list):
				for stat_info in stat_list:
					stat_info = AttrDict(stat_info)
					model.log.warning(f"The following kits had {name}:")
					branch_info = f"{stat_info.name} branch {stat_info.branch}".ljust(30)
					model.log.warning(f"* {branch_info} -- {stat_info.count} {shortname}.")
				model.log.warning(f"{name} errors logged to {model.temp_path}.")
		model.log.debug("display_error_summary end")

	def get_output_sha1s(self):
		output_sha1s = {}
		for job in self.kit_jobs:
			kit_name = job.kit.name
			if kit_name not in output_sha1s:
				output_sha1s[kit_name] = {}
			output_sha1s[kit_name][job.kit.branch] = job.kit_sha1
		return output_sha1s

	def generate_metarepo_metadata(self):
		output_sha1s = self.get_output_sha1s()

		if not os.path.exists(self.meta_repo.root + "/metadata"):
			os.makedirs(self.meta_repo.root + "/metadata")

		with open(self.meta_repo.root + "/metadata/kit-sha1.json", "w") as a:
			a.write(json.dumps(output_sha1s, sort_keys=True, indent=4, ensure_ascii=False))

		outf = self.meta_repo.root + "/metadata/kit-info.json"
		all_kit_names = sorted(output_sha1s.keys())

		with open(outf, "w") as a:
			k_info = {}
			r_defs = {}
			out_settings = defaultdict(lambda: defaultdict(dict))
			for job in self.kit_jobs:
				kit = job.kit
				# specific keywords that can be set for each branch to identify its current quality level
				out_settings[kit.name]["stability"][kit.branch] = kit.stability
				out_settings[kit.name]["type"] = "auto"
				if kit.stability != "deprecated":
					if kit.name not in r_defs:
						r_defs[kit.name] = []
					r_defs[kit.name].append(kit.branch)
			k_info["kit_order"] = all_kit_names
			k_info["kit_settings"] = out_settings

			rel_info = model.release_yaml.get_release_metadata()

			k_info["release_defs"] = r_defs
			k_info["release_info"] = rel_info
			a.write(json.dumps(k_info, sort_keys=True, indent=4, ensure_ascii=False))

		with open(self.meta_repo.root + "/metadata/version.json", "w") as a:
			a.write(json.dumps(rel_info, sort_keys=True, indent=4, ensure_ascii=False))

	async def process_all_kits_in_release(self, method="generate"):
		all_masters = set()
		for kit_name, kit_list in model.release_yaml.kits.items():
			for kit in kit_list:
				all_masters |= set(kit.masters)

		for master in all_masters:
			if not len(model.release_yaml.kits[master]):
				raise ValueError(f"Master {master} defined in release does not seem to exist in kits YAML.")
			elif len(model.release_yaml.kits[master]) > 1:
				raise ValueError(
					f"This release defines {master} multiple times, but it is a master. Only define one master since it is foundational to the release.")

		master_jobs_list = []
		other_jobs_list = []

		for kit_name, kit_list in model.release_yaml.kits.items():
			for kit in kit_list:
				kit_job = KitGenerator(self, kit, is_master=kit_name in all_masters)
				self.kit_jobs.append(kit_job)
				if kit_name in all_masters:
					self.master_jobs[kit_name] = kit_job
				if kit_job.is_master:
					master_jobs_list.append(kit_job)
				else:
					other_jobs_list.append(kit_job)

		master_pool = KitExecutionPool(jobs=master_jobs_list, method=method)
		success = await master_pool.run()
		if not success:
			return False

		other_pool = KitExecutionPool(jobs=other_jobs_list, method=method)
		success = await other_pool.run()
		return success

	async def distfile_sync(self):
		await self.process_all_kits_in_release(method="distfile_scan")

	async def generate(self):
		self.moonbeam_task = asyncio.create_task(self.moonbeam.start())
		model.log.debug(f"moonbeam: {self.moonbeam} {self.moonbeam_task}")
		meta_repo_config = model.release_yaml.get_repo_config("meta-repo")
		self.meta_repo = model.git_class(
			name="meta-repo",
			branch=model.release,
			url=meta_repo_config['url'] if model.prod else None,
			root=model.dest_trees + "/meta-repo",
			origin_check=True if model.prod else None,
			mirrors=meta_repo_config['mirrors'],
			create_branches=model.create_branches,
			model=model,
			**model.git_kwargs
		)

		await self.meta_repo.initialize()
		model.log.debug("In generate() start")
		self.cleanup_error_logs()

		success = await self.process_all_kits_in_release(method="generate")
		if not success:
			self.display_error_summary()
			model.log.debug("FAILURE in process_all_kits_in_release")
			return False

		if not self.write:
			model.log.debug("not doing commit, so exiting from job controller early")
			return True

		# Create meta-repo commit referencing our updated kits:
		self.generate_metarepo_metadata()
		await self.meta_repo.git_commit(message="kit updates", skip=["kits"], push=model.push)

		# TODO: implement this
		# if not model.prod:
		#	# check out preferred kit branches, because there's a good chance we'll be using it locally.
		#	for name, ctx in self.get_kit_preferred_branches().items():
		#		model.log.info(f"Checking out {name} {ctx.kit.branch}...")
		#		await self.checkout_kit(ctx, pull=False)

		if not model.mirror_repos:
			model.log.debug("not mirroring repos, so exiting from job controller early")
			self.display_error_summary()
			return True

		# Mirroring to GitHub happens here:
		if model.push:
			await self.mirror_all_repositories()
		model.log.debug("exiting from job controller")
		self.display_error_summary()
		return True

	async def mirror_repository(self, repo: Tree, base_path, mirror):
		"""
		Mirror a repository to its mirror location, ie. GitHub.
		"""

		os.makedirs(base_path, exist_ok=True)
		await run_shell(f"git clone --bare {repo.root} {base_path}/{repo.name}.pushme", logger=model.log)
		await run_shell(
			f"cd {base_path}/{repo.name}.pushme && git remote add upstream {mirror} && git push --mirror upstream",
			logger=model.log
		)
		await run_shell(f"rm -rf {base_path}/{repo.name}.pushme", logger=model.log)
		return repo.name

	# TODO: this can easily be made faster with gather:
	async def mirror_all_repositories(self):
		base_path = os.path.join(model.temp_path, "mirror_repos")
		await run_shell(f"rm -rf {base_path}", logger=model.log)
		kit_mirror_futures = []
		for kit_job in self.kit_jobs:
			if not kit_job.out_tree.mirrors:
				continue
			kit = kit_job.kit
			for mirror in kit_job.out_tree.mirrors:
				mirror = mirror.format(repo=kit_job.kit.name)
				await self.mirror_repository(kit_job.out_tree, base_path, mirror)
		for mirror in self.meta_repo.mirrors:
			mirror = mirror.format(repo=self.meta_repo.name)
			await self.mirror_repository(self.meta_repo, base_path, mirror)
		model.log.info("Mirroring of meta-repo complete.")


"""
class MetaRepoGenerator:

	def __init__(self):



# TODO: integrate this into the workflow

"""
