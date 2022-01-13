import os
from collections import OrderedDict, defaultdict
from datetime import datetime

import yaml
from subpop.config import ConfigurationError

from metatools.context import GitRepositoryLocator
from metatools.yaml_util import YAMLReader

"""
This file contains classes used to create an object model for the contents of a releases/<release>.yaml file,
to more easily interact with the logical contents of this file without having to know the intricacies of the
actual file format.
"""


class SourceRepository:

	"""
	This SourceRepository represents a single source repository referenced in the YAML. This source repository
	is used as a source tree for copying in ebuilds and eclasses into a kit.
	"""

	def __init__(self, name=None, copyright=None, url=None, eclasses=None, src_sha1=None, branch=None, notes=None):
		self.name = name
		self.copyright = copyright
		self.url = url
		self.eclasses = eclasses
		self.src_sha1 = src_sha1
		self.branch = branch if branch else "master"
		self.notes = notes
		# This can be used to track a GitTree associated with the source repository.
		self.tree = None

	def is_equivalent(self, other):
		"""
		This allows comparison of source repositories. We don't ensure singletons on source repos so this allows
		the auto-checking-out of source collections to see if the repo is the 'same' as a previously-checked-out
		repo:
		"""
		if not isinstance(other, SourceRepository):
			return NotImplementedError()
		return self.name == other.name and self.url == other.url and self.src_sha1 == other.src_sha1 and self.branch == other.branch


class SourceCollection:

	"""
	A SourceCollection in the YAML is, as the name says, a collection of source repositories. Each kit can reference
	one SourceCollection and copy ebuilds and eclasses from the SourceRepositories defined in each collection.
	"""

	def __init__(self, name, repositories=None):
		self.name = name
		self.repositories = {}
		for repo in repositories:
			self.repositories[repo.name] = repo


class Kit:

	"""
	This class represents Kit defined in the release's YAML. It contains settings from the YAML data related to how the
	kit should be assembled. It does not contain a reference to the actual Git repository of the kit, as it is just designed
	as an object model of the settings for the Kit.
	"""

	# These are populated later, when the KitGenerator initializes, and contain paths to eclasses defined in this kit, along with md5's for them:
	eclass_paths = None
	eclass_hashes = None
	is_master = False

	def __init__(self, locator, release=None, name=None, source : SourceCollection = None, stability=None, branch=None, eclasses=None, priority=None, aliases=None, masters=None, sync_url=None, settings=None):
		self.kit_fixups: GitRepositoryLocator = locator
		self.release = release
		self.name = name
		self.source = source
		self.stability = stability
		self.branch = branch
		self.eclasses = eclasses if eclasses is not None else {}
		self.priority = priority
		self.aliases = aliases if aliases else []
		self.masters = masters if masters else []
		# This will be initialized by ReleaseYAML.set_kit_hierarchies() later, once all Kits have been instantiated:
		self.masters_list = []
		self.sync_url = sync_url.format(kit_name=name) if sync_url else None
		self.settings = settings if settings is not None else {}
		self._package_data = None
		self.eclass_hashes = {}
		self.eclass_paths = {}

	@property
	def package_data(self):
		if self._package_data is None:
			self._package_data = self._get_package_data()
		return self._package_data

	def _get_package_data(self):

		# Look for branch-specific packages.yaml:
		fn = f"{self.kit_fixups.root}/{self.name}/{self.branch}/packages.yaml"
		# Fallback to curated packages.yaml:
		if not os.path.exists(fn):
			fn = f"{self.kit_fixups.root}/{self.name}/curated/packages.yaml"
		# Fallback to kit-wide packages.yaml:
		if not os.path.exists(fn):
			fn = f"{self.kit_fixups.root}/{self.name}/packages.yaml"
		with open(fn, "r") as f:
			return yaml.safe_load(f)

	def yaml_walk(self, yaml_dict):
		"""
		This method will scan a section of loaded YAML and return all list elements -- the leaf items.
		"""
		retval = []
		for key, item in yaml_dict.items():
			if isinstance(item, dict):
				retval += self.yaml_walk(item)
			elif isinstance(item, list):
				retval += item
			else:
				raise TypeError(f"yaml_walk: unrecognized: {repr(item)}")
		return retval

	def get_kit_items(self, section="packages"):
		if section in self.package_data:
			for package_set in self.package_data[section]:
				repo_name = list(package_set.keys())[0]
				if section == "packages":
					# for packages, allow arbitrary nesting, only capturing leaf nodes (catpkgs):
					yield repo_name, self.yaml_walk(package_set)
				else:
					# not a packages section, and just return the raw YAML subsection for further parsing:
					packages = package_set[repo_name]
					yield repo_name, packages

	def get_kit_packages(self):
		return self.get_kit_items()

	def get_excludes(self):
		"""
		Grabs the excludes: section from packages.yaml, which is used to remove stuff from the resultant
		kit that accidentally got copied by merge scripts (due to a directory looking like an ebuild
		directory, for example.)
		"""
		if "exclude" in self.package_data:
			return self.package_data["exclude"]
		else:
			return []

	def get_individual_files_to_copy(self):
		"""
		Parses the 'eclasses' and 'copyfiles' sections in a kit's YAML and returns a list of files to
		copy from each source repository in a tuple format.
		"""
		#TODO: upgrade ability to specify eclass items in packages.yaml.

		eclass_items = list(self.get_kit_items(section="eclasses"))
		copyfile_items = list(self.get_kit_items(section="copyfiles"))
		copy_tuple_dict = defaultdict(list)

		for src_repo, eclasses in eclass_items:
			for eclass in eclasses:
				copy_tuple_dict[src_repo].append((f"eclass/{eclass}.eclass", f"eclass/{eclass}.eclass"))

		for src_repo, copyfiles in copyfile_items:
			for copy_dict in copyfiles:
				copy_tuple_dict[src_repo].append((copy_dict["src"], copy_dict["dest"] if "dest" in copy_dict else copy_dict["src"]))
		return copy_tuple_dict

	def get_copyright_rst(self):
		cur_year = str(datetime.now().year)
		out = self.release.get_default_copyright_rst().replace("{{cur_year}}", cur_year)
		for source_name in sorted(self.source.repositories.keys()):
			source = self.source.repositories[source_name]
			if source.copyright:
				out += source.copyright.replace("{{cur_year}}", cur_year)
		return out


class ReleaseYAML(YAMLReader):
	"""
	This class is the primary object created from a releases/<release>.yaml file, and contains an object hierarchy
	that defines a release.

	The purpose of this object is to make it easy to obtain information in this file, properly parsed and interpreted,
	ready for use. Other parts of code should use this class to access release.yaml data rather than touching it directly.

	All the info in release.yaml is parsed and an object tree is built to represent the information in the file.

	When a ReleaseYAML object is instantiated, the following sub-objects are created:

	1. The self.source_collections attribute is an OrderedDict containing all source collections, indexed by their
	   name. Each source collection has a repositories attribute containing an OrderedDict of repositories associated
	   with the source collection, in reverse priority order (later OrderedDict elements have priority over earlier
	   elements.)

	2. self.kits will contain an ordered list of kits in the release. kit.source will be initialized to point to
	   the live source collection object associated with the kit, which contains references to the repositories that
	   can be used by the kit.yaml to reference ebuilds to copy into this kit.
	"""

	source_collections = None
	kits = None
	filename = None
	remotes = None
	locator = None
	masters = None

	def start(self):
		self.kits = self._kits()
		self.set_kit_hierarchies()
		self.remotes = self._remotes()

	def set_kit_hierarchies(self):
		"""
		For each kit in self.kits, we want to give them references to their masters, if any. That way, they have a
		model of this in self.masters_list. This also performs some validation -- like we currently don't allow multiple
		definitions of a master. If something's a master, the YAML should only have one branch of it defined per
		release since this kit is 'foundational' for the release.
		"""

		all_masters = set()
		for kit_name, kit_list in self.kits.items():
			for kit in kit_list:
				all_masters |= set(kit.masters)

		# validation --

		for master in all_masters:
			if not len(self.kits[master]):
				raise ValueError(f"Master {master} defined in release does not seem to exist in kits YAML.")
			elif len(self.kits[master]) > 1:
				raise ValueError(f"This release defines {master} multiple times, but it is a master. Only define one master since it is foundational to the release.")

		# Used in kit job planning, let's set an is_master boolean for each kit.
		self.masters = {}
		for master in all_masters:
			self.kits[master][0].is_master = True

		# We now know that we have only one master defined in the yaml. So we can reference it in position 0:

		for kit_name, kit_list in self.kits.items():
			for kit in kit_list:
				for master in kit.masters:
					kit.masters_list.append(self.kits[master][0])

		# Now each repo can access its masters at self.masters_list.

	def get_default_copyright_rst(self):
		return self.get_elem("release/copyright")

	def get_meta_repo_config(self):
		"""
		Return the remote for meta-repo based on whether we are running in dev or prod mode.
		"""
		if self.mode not in self.remotes:
			raise ConfigurationError(f"No remotes defined for '{self.mode}' in {self.filename}.")
		if 'meta-repo' not in self.remotes[self.mode]:
			raise ConfigurationError(f"No remote 'meta-repo' defined for '{self.mode}' in {self.filename}.")
		if 'url' not in self.remotes[self.mode]['meta-repo']:
			raise ConfigurationError(f"No remote 'meta-repo' URL defined for '{self.mode}' in {self.filename}.")
		mirrs = []
		if 'mirrors' in self.remotes[self.mode]['meta-repo']:
			mirrs = self.remotes[self.mode]['meta-repo']['mirrors']
		return {
			"url": self.remotes[self.mode]['meta-repo']['url'],
			"mirrors": mirrs
		}

	def get_kit_config(self, kit_name):
		"""
		Given a kit named ``kit_name``, determine its remote based on whether we are running in dev or prod mode.
		"""
		if self.mode not in self.remotes:
			raise ConfigurationError(f"No remotes defined for '{self.mode}' in {self.filename}.")
		if 'kits' not in self.remotes[self.mode]:
			raise ConfigurationError(f"No remote 'kits' defined for '{self.mode}' in {self.filename}.")
		if 'url' not in self.remotes[self.mode]['kits']:
			raise ConfigurationError(f"No remote 'kits' URL defined for '{self.mode}' in {self.filename}.")
		mirrs = []
		if 'mirrors' in self.remotes[self.mode]['kits']:
			for mirr in self.remotes[self.mode]['kits']['mirrors']:
				mirrs.append(mirr)
		return {
			"url": self.remotes[self.mode]['kits']['url'].format(kit_name=kit_name),
			"mirrors": mirrs
		}

	def __init__(self, locator: GitRepositoryLocator, release=None, mode="dev"):
		self.locator = locator
		assert release is not None
		filename = f'{locator.root}/releases/{release}.yaml'
		if not os.path.exists(filename):
			raise ConfigurationError(f"Cannot find expected {filename}")
		self.mode = mode
		self.filename = filename
		with open(filename, 'r') as f:
			super().__init__(f)

	def _repositories(self):
		"""
		This is an internal helper method to return the master list of repositories. It should not be used by other parts
		outside this code because this master list can be tweaked by the data that appears in self.source_collections().
		Thus, self.source_collections() should be used as the authoritative definition of repositories, not this particular
		data.
		"""
		repos = OrderedDict()
		for yaml_dat in self.iter_list("release/repositories"):
			name = list(yaml_dat.keys())[0]
			kwargs = yaml_dat[name]
			repos[name] = kwargs
		return repos

	def _source_collections(self):
		"""
		A kit's packages.yaml file can be used to reference catpkgs in external overlays, as well as eclasses,
		that should be copied into the kit when it is generated. This group of source repositories is called a
		'source collection', and is  represented by a SourceCollection object.

		One source collection is mapped to each kit in a release, in the release.yaml file 'source' YAML element.
		A source collection has one or more repositories defined. Each source repository is represented by a
		SourceRepository object.

		This method returns an OrderedDict() of all SourceCollections defined in the YAML, which is indexed by
		the YAML name of the source collection. Each kit defined in the YAML can reference one of these source
		collections by name.

		When kits are parsed by the self.kits() method, the source collection referenced by each kit will be
		passed to the kit's constructor.
		"""
		source_collections = OrderedDict()
		repositories = self._repositories()
		for collection_name, collection_items in self.iter_groups("release/source-collections"):
			collection_objs = []
			names = set()
			for repo_def in collection_items:
				repo_name = None
				if isinstance(repo_def, str):
					# str -> actual pre-defined repository dict
					repo_name = repo_def
					repo_def = repositories[repo_def]
				elif isinstance(repo_def, dict):
					# use pre-defined repository as base and augment with any local tweaks
					repo_name = list(repo_def.keys())[0]
					repo_dict = repo_def[repo_name]
					repo_def = repositories[repo_name].copy()
					repo_def.update(repo_dict)
				if repo_name in names:
					raise ValueError(f"Duplicate repository name {repo_name} in source collection {collection_name}")
				names.add(repo_name)
				repo_obj = SourceRepository(name=repo_name, **repo_def)
				collection_objs.append(repo_obj)
			source_collections[collection_name] = SourceCollection(name=collection_name, repositories=collection_objs)
		return source_collections

	def _remotes(self):
		return self.get_elem("release/remotes")

	def _kits(self):
		"""
		Returns a defaultdict[list] mapping each kit name to the kit data in the JSON, where multiple kits with the same name
		will appear in the list in the order they appear in the YAML. We generally consider the first kit to be the 'primary'
		(active) kit.
		"""
		collections = self._source_collections()
		kits = defaultdict(list)
		kit_defaults = self.get_elem("release/kit-definitions/defaults")
		if kit_defaults is None:
			kit_defaults = {}
		for kit_el in self.iter_list("release/kit-definitions/kits"):
			kit_insides = kit_defaults.copy()
			kit_name = None
			if isinstance(kit_el, str):
				kit_name = kit_el
			elif isinstance(kit_el, dict):
				kit_name = list(kit_el.keys())[0]
				kit_insides.update(kit_el[kit_name])
			if 'source' in kit_insides:
				sdef_name = kit_insides['source']
				# convert from string to actual SourceCollection Object
				try:
					kit_insides['source'] = collections[sdef_name]
				except KeyError:
					raise KeyError(f"Source collection '{sdef_name}' not found in source-definitions section of release.yaml.")
			kits[kit_name].append(Kit(self.locator, release=self, name=kit_name, **kit_insides))
		return kits

	def iter_kits(self, name=None):
		"""
		This is a handy way to iterate over all kits that meet certain criteria (currently supporting kit
		name.) This is used to get all python-kit kits for auto-USE-flag generation.
		"""
		for kit_name, kit_list in self.kits.items():
			if name is not None and kit_name != name:
				continue
			for kit in kit_list:
				yield kit


if __name__ == "__main__":
	locator = GitRepositoryLocator()
	ryaml = ReleaseYAML(locator, mode="prod")
	print("REMOTES", ryaml.remotes)
	print(ryaml.get_kit_remote("core-kit"))

