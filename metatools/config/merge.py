import os
from collections import OrderedDict, defaultdict
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from enum import Enum

import yaml

from metatools.config.base import MinimalConfig
from metatools.context import GitRepositoryLocator
from metatools.tree import AutoCreatedGitTree
from metatools.tree import GitTree
from metatools.yaml_util import YAMLReader
from subpop.config import ConfigurationError

"""
This file contains classes used to create an object model for the contents of a releases/<release>.yaml file,
to more easily interact with the logical contents of this file without having to know the intricacies of the
actual file format.
"""


class MinimalMergeConfig(MinimalConfig):
	"""
	This configuration is for minimal tools that use merge-related data, like deepdive, for example.
	Deepdive can use this to access the release YAML without worrying about more complex data.
	"""

	# Configuration bits:

	release_yaml = None
	release = None
	context = None
	locator = None
	fixups_url = None
	fixups_branch = None
	# Things used during runtime processing:
	kit_fixups: GitTree = None
	logger_name = "metatools.merge"
	log = None
	debug = False

	async def initialize(self, release=None, fixups_url=None, fixups_branch=None, debug=False):
		await super().initialize(debug=debug)
		self.log.debug("Trying to find kit-fixups")
		# TODO: refuse to use any source repository that has local changes (use git status --porcelain | wc -l)
		self.context = os.path.join(self.source_trees, "kit-fixups")
		self.kit_fixups = GitTree(
			name='kit-fixups',
			root=self.context,
			model=self,
			url=fixups_url,
			branch=fixups_branch,
			keep_branch=True
		)
		self.log.debug("Initializing kit-fixups repository in model init")
		await self.kit_fixups.initialize()
		self.locator = GitRepositoryLocator(start_path=self.kit_fixups.root)
		self.release = release
		self.release_yaml = ReleaseYAML(self)


class MergeConfig(MinimalMergeConfig):
	"""
	This configuration is used for tree regen, also known as 'merge-kits'.
	"""

	meta_repo = None
	prod = False
	push = False
	create_branches = False
	mirror_repos = False
	nest_kits = True
	git_class = AutoCreatedGitTree
	git_kwargs = {}
	howdy = False

	# TODO: should probably review the error/warning stats variables here:
	metadata_error_stats = []
	processing_warning_stats = []
	processing_error_stats = []
	start_time: datetime = None
	current_source_def = None

	async def initialize(self, prod=False, push=False, release=None, create_branches=False, fixups_url=None,
						 fixups_branch=None, debug=False, howdy=False):

		self.prod = prod
		self.push = push
		self.create_branches = create_branches
		self.howdy = howdy

		# TODO: add a means to override the remotes in the release.yaml using a local config file.

		if not self.prod:
			# The ``push`` keyword argument only makes sense in prod mode. If not in prod mode, we don't push.
			self.push = False
		else:
			# In this mode, we're actually wanting to update real kits, and likely are going to push our updates to remotes (unless
			# --nopush is specified as an arg.) This might be used by people generating their own custom kits for use on other systems,
			# or by Funtoo itself for updating official kits and meta-repo.
			self.push = push
			self.nest_kits = False
			self.mirror_repos = push
			self.git_class = GitTree
			self.git_kwargs = {"checkout_all_branches": True}

		await super().initialize(release=release, fixups_url=fixups_url, fixups_branch=fixups_branch, debug=debug)
		self.log.debug("Model initialization complete.")


class SourceRepository:
	"""
	This SourceRepository represents a single source repository referenced in the YAML. This source repository
	is used as a source tree for copying in ebuilds and eclasses into a kit.
	"""

	def __init__(self, yaml=None, name=None, copyright=None, url=None, eclasses=None, src_sha1=None, branch=None,
				 notes=None):
		self.yaml = yaml
		assert yaml is not None
		self.name = name
		self.copyright = copyright
		self.url = url
		self.eclasses = eclasses
		self.notes = notes
		# This can be used to track a GitTree associated with the source repository.
		self.tree = None
		self.src_sha1 = src_sha1
		self.branch = branch
		self.initialized = False

	async def initialize(self):
		# This is a simple source repository -- we only want to initialize it once:
		if self.initialized:
			return
		self.yaml.model.log.info(
			f"Initializing: Source Repository {self.name} branch: {self.branch} SHA1: {self.src_sha1} {self.url}")
		self.tree = GitTree(
			self.name,
			url=self.url,
			root="%s/%s" % (self.yaml.model.source_trees, self.name),
			branch=self.branch,
			commit_sha1=self.src_sha1,
			origin_check=False,
			reclone=False,
			model=self.yaml.model
		)
		await self.tree.initialize()
		self.initialized = True

	def find_license(self, license):
		try:
			return self.tree.find_license(license)
		except FileNotFoundError:
			self.yaml.model.log.error(f"No license named '{license}' found in SourceRepository {self.name}")


class SharedSourceRepository(SourceRepository):
	"""
	SharedSourceRepository is a source repository referenced by a source collection. Source collections can be shared.

	Different kits can use different snapshots of the same source repository, so special care needs to be given to
	allowing a re-initialization of the tree so it is on the proper SHA1/branch. This is done by the initialize() call
	rather than the constructor.

	There is special code in here to attempt to not unnecessarily re-initialize git repositories that are already ready
	for use. SharedSourceRepository objects can be used by multiple SourceCollections. There is a 1:1 mapping between
	SharedSourceRepository and the underlying GitTree() object. So we only have one GitTree() for a particular repo,
	such as a gentoo-staging repo, even if different source collections leverage different SHA1 snapshots.
	"""

	def __init__(self, yaml=None, name=None, copyright=None, url=None, eclasses=None, notes=None):
		self.yaml = yaml
		assert yaml is not None
		self.name = name
		self.copyright = copyright
		self.url = url
		self.eclasses = eclasses
		self.notes = notes
		# This can be used to track a GitTree associated with the source repository.
		self.tree = None

	async def initialize(self, branch=None, src_sha1=None):
		if self.tree:
			if (branch is None or self.tree.branch == branch) and src_sha1 == self.tree.commit_sha1:
				self.yaml.model.log.info(
					f"Keeping existing source repository {self.name} branch: {self.tree.branch} SHA1: {self.tree.commit_sha1} {self.url}")
				return
			else:
				self.yaml.model.log.info(
					f"src repo {self.name}: initialize: {self.tree.branch}/{self.tree.commit_sha1} -> {branch}/{src_sha1}")
				self.yaml.model.log.info(
					f"Checkout: Source Repository {self.name} branch: {branch} SHA1: {src_sha1} {self.url}")
				await self.tree.git_checkout(branch=branch, sha1=src_sha1)
		else:
			self.yaml.model.log.info(
				f"Initializing: Source Repository {self.name} branch: {branch} SHA1: {src_sha1} {self.url}")
			self.tree = GitTree(
				self.name,
				url=self.url,
				root="%s/%s" % (self.yaml.model.source_trees, self.name),
				branch=branch,
				commit_sha1=src_sha1,
				origin_check=False,
				reclone=False,
				model=self.yaml.model
			)
			await self.tree.initialize()


class SourceCollection:
	"""
	A SourceCollection in the YAML is, as the name says, a collection of source repositories, and it's worth
	noting that SourceCollections aren't used by sourced kits at all. So this logic all assumes we are dealing
	with auto-generated kits that can reference multiple repos in their packages.yaml.
	"""

	def __init__(self, name=None, yaml=None, repo_defs=None):
		self.yaml = yaml
		self.name = name
		# Contains abstract definitions of repos, which we can instantiate
		self.repo_defs = repo_defs
		self.repositories = OrderedDict()

	def find_license(self, license):
		for repo in reversed(self.repositories.keys()):
			try:
				license = self.repositories[repo].tree.find_license(license)
			except FileNotFoundError:
				continue
			return license
		self.yaml.model.log.error(f"No license named '{license}' found in SourceCollection {self.name}")

	async def initialize(self, repo_names=None):

		"""
		This method initializes the source repositories referenced by the kit to ensure that they are all initialized to the
		proper branch and/or SHA1. Some internal checking is done to avoid re-initializing repositories unnecessarily, so if
		they are already set up properly then no action will be taken.

		Note: Due to the nature of what we're doing, these repositories are all SharedSourceRepositories.
		"""

		for repo_name, repo_def in self.repo_defs.items():
			# Skip any repos that we aren't using right now....
			if repo_names is not None and repo_name not in repo_names:
				continue
			# If repo already exists, don't create it from scratch. Should be faster:
			if repo_name in self.yaml.all_repo_objs:
				self.repositories[repo_name] = self.yaml.all_repo_objs[repo_name]
			else:
				# note that src_sha1 and branch get passed as keyword arguments to initialize() in the next loop.
				kwargs = repo_def.copy()
				for arg in ["src_sha1", "branch"]:
					if arg in kwargs:
						del kwargs[arg]
				self.yaml.all_repo_objs[repo_name] = self.repositories[repo_name] = SharedSourceRepository(**kwargs,
																										   yaml=self.yaml,
																										   name=repo_name)
		for repo_name, repo in self.repositories.items():
			branch = None
			src_sha1 = None
			if "src_sha1" in self.repo_defs[repo_name]:
				src_sha1 = self.repo_defs[repo_name]["src_sha1"]
			if "branch" in self.repo_defs[repo_name]:
				branch = self.repo_defs[repo_name]["branch"]
			await repo.initialize(branch=branch, src_sha1=src_sha1)
		self.yaml.model.current_source_def = self


class Kit:
	"""
	This class represents Kit defined in the release's YAML. It contains settings from the YAML data related to how the
	kit should be assembled. It does not contain a reference to the actual Git repository of the kit, as it is just designed
	as an object model of the settings for the Kit.

	Don't use the class directly. Use ``SourcedKit()`` or ``AutoGeneratedKit()``, below.
	"""

	source = None

	def __init__(self, locator, release=None, name=None, stability=None, branch=None, eclasses=None, priority=None,
				 aliases=None, masters=None, sync_url=None, settings=None):
		self.kit_fixups: GitRepositoryLocator = locator
		assert self.kit_fixups is not None
		self.release = release
		self.name = name
		# For a sourced kit, this is a SourceRepository. For an autogenerated kit, it is a collection of SourceRepositories (SourceCollection):
		self.stability = stability
		self.branch = branch
		self.eclasses = eclasses if eclasses is not None else {}
		self.priority = priority
		self.aliases = aliases if aliases else []
		self.masters = masters if masters else []
		self.sync_url = sync_url.format(kit_name=name) if sync_url else None
		self.settings = settings if settings is not None else {}

	async def initialize_sources(self):
		pass

	def get_copyright_rst(self):
		cur_year = str(datetime.now().year)
		out = self.release.get_default_copyright_rst().replace("{{cur_year}}", cur_year)
		if isinstance(self, AutoGeneratedKit):
			for source_name in sorted(self.source.repositories.keys()):
				source = self.source.repositories[source_name]
				if source.copyright:
					out += source.copyright.replace("{{cur_year}}", cur_year)
		elif isinstance(self, SourcedKit):
			if self.source.copyright:
				out += self.source.copyright.replace("{{cur_year}}", cur_year)
		else:
			raise TypeError("Unrecognized kit format")
		return out


class SourcedKit(Kit):
	source: SourceRepository = None

	def __init__(self, source: SourceRepository = None, **kwargs):
		super().__init__(**kwargs)
		self.source = source

	async def initialize_sources(self):
		await self.source.initialize()


class AutoGeneratedKit(Kit):
	_package_data = None
	source: SourceCollection = None

	def __init__(self, source: SourceCollection = None, **kwargs):
		super().__init__(**kwargs)
		self.source = source

	@property
	def package_data(self):
		if self._package_data is None:
			self._package_data = self._get_package_data()
		return self._package_data

	async def initialize_sources(self):
		"""
		This method is used to get the SourceCollection's SharedSourceRepository objects initialized so we are ready to copy ebuilds/eclasses from
		the right branch/SHA1.

		The use of repo_names exists to inform the initialize() call of what repos we are actually going to use. There is no point in performing
		significant IO to initialize repos that we are not actually using.
		"""
		repo_names = []
		for repo_name, extra in self.get_kit_items():
			repo_names.append(repo_name)
		for repo_name, extra in self.get_kit_items(section="copyfiles"):
			repo_names.append(repo_name)
		for repo_name, extra in self.get_kit_items(section="eclasses"):
			repo_names.append(repo_name)
		await self.source.initialize(repo_names=repo_names)

	@property
	def packages_yaml(self):
		# Look for branch-specific packages.yaml:
		fn = self.specific_packages_yaml
		# Fallback to curated packages.yaml:
		if not os.path.exists(fn):
			fn = f"{self.kit_fixups.root}/{self.name}/curated/packages.yaml"
		# Fallback to kit-wide packages.yaml:
		if not os.path.exists(fn):
			fn = f"{self.kit_fixups.root}/{self.name}/packages.yaml"
		return fn

	@property
	def specific_packages_yaml(self):
		return f"{self.kit_fixups.root}/{self.name}/{self.branch}/packages.yaml"

	def _get_package_data(self):
		with open(self.packages_yaml, "r") as f:
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

	def eclass_include_info(self):
		"""
		This method parses the release yaml and returns the logical information related to what eclasses, from what
		source repositories, should be included. We also return a mask set of eclasses that should definitely not
		be included, and any eclasses matching these names will be excluded. A special value of '*' means to include
		the full tree of eclasses from a source repository.

		What is returned is a dictionary in the following format::

		  {
		    "mask" : set() of masks,
		  	"include" : {
		  		"source_repo_name_1" : [
		  			"name_of_eclass_without_.eclass_extension",
		  		],
		  		"source_repo_name_2: : [
		  			"*",
		  		]
		  }
		"""

		if "mask" in self.eclasses:
			mask_set = set(list(self.eclasses["mask"]))
		else:
			mask_set = set()

		return {
			"mask": mask_set,
			"include": self.eclasses["include"] if "include" in self.eclasses else {}
		}

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

	def get_kit_packages(self):
		return self.get_kit_items()


class KitKind(Enum):
	AUTOGENERATED = "auto"
	SOURCED = "sourced"


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
	masters = None
	all_repo_objs = dict()

	def __init__(self, model: MinimalMergeConfig):
		self.model = model
		if isinstance(model, MergeConfig):
			self.mode = "prod" if self.model.prod is True else "dev"
		filename = f'{self.model.locator.root}/releases/{self.model.release}/repositories.yaml'
		if not os.path.exists(filename):
			raise ConfigurationError(f"Cannot find expected {filename}")
		self.filename = filename
		with open(filename, 'r') as f:
			super().__init__(f)

	def start(self):
		self.kits = self._kits()
		self.remotes = self._remotes()

	def get_default_copyright_rst(self):
		return self.get_elem("release/copyright")

	def get_release_metadata(self):
		return self.get_elem("release/metadata")

	def get_repo_config(self, repo_name):
		"""
		Given a repo/kit named ``repo_name``, determine its remote based on whether we are running in dev or prod mode.
		"""
		if self.mode is None:
			raise NotImplementedError("To use ReleaseYAML.get_repo_config(), use a MergeConfig() rather than MinimalMergeConfig()")

		if self.mode not in self.remotes:
			raise ConfigurationError(f"No remotes defined for '{self.mode}' in {self.filename}.")
		if 'url' not in self.remotes[self.mode]:
			raise ConfigurationError(f"No URL defined for '{self.mode}' in {self.filename}.")
		self.model.log.debug(f"get_repo_config: self.mode {self.mode} url: {self.remotes[self.mode]}")
		mirrs = []
		if 'mirrors' in self.remotes[self.mode]:
			for mirr in self.remotes[self.mode]['mirrors']:
				mirrs.append(mirr)
		return {
			"url": self.remotes[self.mode]['url'].format(repo=repo_name),
			"mirrors": mirrs
		}

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

		One source collection is mapped to each auto-generated kit in a release, in the release.yaml file
		'source' YAML element. A source collection has one or more repositories defined. Each source repository
		is represented by a SourceRepository object.

		This method returns an OrderedDict() of all SourceCollections defined in the YAML, which is indexed by
		the YAML name of the source collection. Each kit defined in the YAML can reference one of these source
		collections by name.

		When kits are parsed by the self.kits() method, the source collection referenced by each kit will be
		passed to the kit's constructor.
		"""
		source_collections = OrderedDict()
		repositories = self._repositories()
		for collection_name, collection_items in self.iter_groups("release/source-collections"):
			names = set()
			repo_defs = OrderedDict()
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
					if repo_name not in repositories:
						raise KeyError(
							f"Referenced repository '{repo_name}' in source collection '{collection_name}' not found in repositories list.")
					repo_def = repositories[repo_name].copy()
					repo_def.update(repo_dict)
				if repo_name in names:
					raise ValueError(f"Duplicate repository name {repo_name} in source collection {collection_name}")
				names.add(repo_name)
				repo_defs[repo_name] = repo_def
			source_collections[collection_name] = SourceCollection(yaml=self, name=collection_name, repo_defs=repo_defs)
			self.model.log.info(f"Added to source collection {collection_name}:\n  {repo_defs}")
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

			# This part of the code handles parsing the YAML, and creating Kit objects, which contain the proper info
			# within to reference the proper source repositories or source repository (in the case of sourced kits.)

			kind = KitKind.AUTOGENERATED if "kind" not in kit_insides else KitKind(kit_insides["kind"])
			if "kind" in kit_insides:
				del kit_insides["kind"]
			if 'source' not in kit_insides:
				raise KeyError(f"source value for kit {kit_name} not defined -- this is likely an error.")
			if kind == KitKind.AUTOGENERATED:
				# autogenerated kits have kit_insides['source'] set to reference a SourceCollection object.
				sdef_name = kit_insides['source']
				# convert from string to actual SourceCollection Object
				try:
					kit_insides['source'] = collections[sdef_name]
				except KeyError:
					raise KeyError(
						f"Source collection '{sdef_name}' not found in source-definitions section of release.yaml.")
				kits[kit_name].append(
					AutoGeneratedKit(locator=self.model.locator, release=self, name=kit_name, **kit_insides))
			elif kind == KitKind.SOURCED:
				# sourced kits have kit_insides['source'] set to reference a SourceRepository object.
				if not isinstance(kit_insides['source'], dict):
					raise ValueError(
						f"source: definition for kit {kit_name} must be a dictionary with 'url' and 'branch' defined. Got this instead: {kit_insides['source']}")
				for key in ["url"]:
					if key not in kit_insides['source']:
						raise KeyError(f"element '{key}' missing from {kit_name} kit definition.")
				if "branch" not in kit_insides['source'] and "src_sha1" not in kit_insides['source']:
					raise KeyError(f"{kit_name} kit definition must define 'src_sha1' or 'branch' under 'source'.")
				if "branch" in kit_insides['source'] and "src_sha1" in kit_insides['source']:
					raise KeyError(
						f"{kit_name} kit definition must define one of 'src_sha1' or 'branch' under 'source'.")
				s_branch = kit_insides["source"].get("branch", None)
				s_src_sha1 = kit_insides["source"].get("src_sha1", None)
				kit_insides['source'] = SourceRepository(yaml=self, name=f"{kit_name}-sources",
														 url=kit_insides['source']['url'], branch=s_branch,
														 src_sha1=s_src_sha1)
				kits[kit_name].append(
					SourcedKit(locator=self.model.locator, release=self, name=kit_name, **kit_insides))
			else:
				raise KeyError(f"Unknown kit kind '{kind}'")
		return kits

	def iter_kits(self, name=None, primary=None):
		"""
		This is a handy way to iterate over all kits that meet certain criteria. By default, this will yield
		individual kits, with the primary kit listed first (if there are multiple kits of the same name)

		If 'name' is specified, then only yield those kits with matching name.
		If 'primary' is True, then yield only primary kits (first kit in YAML).
		"""
		for kit_name, kit_list in self.kits.items():
			if name is not None and kit_name != name:
				continue
			if primary:
				yield kit_list[0]
			else:
				for kit in kit_list:
					yield kit

# vim: ts=4 sw=4 noet
