import os
import threading
from collections import defaultdict
from datetime import datetime

import yaml

from metatools.config.base import MinimalConfig
from metatools.context import GitRepositoryLocator
from metatools.files.release import ReleaseYAML
from metatools.hashutils import get_md5
from metatools.tree import AutoCreatedGitTree, GitTree
from subpop.config import ConfigurationError


class EClassHashCollector:

	LOCK = threading.Lock()
	# mapping eclass to source location:
	eclass_loc_dict = {}
	# mapping eclass to hash:
	eclass_hash_dict = {}

	"""
	When we are doing a merge run, we need to collect the hashes for all the eclasses in each kit. We also
	need to ensure that eclasses only appear once and are not duplicated (best practice, and not doing so
	creates problems with inconsistent behavior.) This class implements a cross-thread storage that can be
	used to record this information and identify when we have a duplicate eclass situation so we can print
	an informative error message.
	"""

	def add_eclasses(self, eclass_sourcedir: str):
		"""

		For generating metadata, we need md5 hashes of all eclasses for writing out into the metadata.

		This function grabs all the md5sums for all eclasses.
		"""

		ecrap = os.path.join(eclass_sourcedir, "eclass")
		if os.path.isdir(ecrap):
			for eclass in os.listdir(ecrap):
				if not eclass.endswith(".eclass"):
					continue
				eclass_path = os.path.join(ecrap, eclass)
				eclass_name = eclass[:-7]
				with self.LOCK:
					if eclass_name in self.eclass_loc_dict:
						raise KeyError(f"Eclass {eclass_name} in {eclass_path} is duplicated by {self.eclass_loc_dict[eclass_name]}. This should be fixed.")
					self.eclass_loc_dict[eclass_name] = eclass_path
					self.eclass_hash_dict[eclass_name] = get_md5(eclass_path)


class MergeConfig(MinimalConfig):
	"""
	This configuration is used for tree regen, also known as 'merge-kits'.
	"""

	release_yaml = None
	context = None
	meta_repo = None
	prod = False
	release = None
	push = False
	create_branches = False

	fastpull = None
	_third_party_mirrors = None

	mirror_repos = False
	nest_kits = True
	git_class = AutoCreatedGitTree

	metadata_error_stats = []
	processing_error_stats = []

	# TODO: need new variables for this since we do this differently with llvm-kit in the mix:
	#       This is used to grab a reference to the eclasses in core kit during regen:
	#eclass_root = None
	#eclass_hashes = None
	eclass_hashes = EClassHashCollector()
	start_time: datetime = None

	async def initialize(self, prod=False, push=False, release=None, create_branches=False):

		self.prod = prod
		self.push = push
		self.release = release
		self.create_branches = create_branches

		# Locate the root of the git repository we're currently in. We assume this is kit-fixups:
		self.context = GitRepositoryLocator().context

		# Next, find release.yaml in the proper directory in kit-fixups. Pass it here:

		release_yaml_fn = os.path.join(self.context, release, "release.yaml")
		if not os.path.exists(release_yaml_fn):
			raise ConfigurationError(f"Cannot find expected {release_yaml_fn}")

		self.release_yaml = ReleaseYAML(release_yaml_fn, mode="prod" if prod else "dev")

		# TODO: add a means to override the remotes in the release.yaml using a local config file.
		# TODO: where do we specify branches in release.yaml... need to add.

		if not self.prod:
			# The ``push`` keyword argument only makes sense in prod mode. If not in prod mode, we don't push.
			self.push = False
		else:

			# In this mode, we're actually wanting to update real kits, and likely are going to push our updates to remotes (unless
			# --nopush is specified as an arg.) This might be used by people generating their own custom kits for use on other systems,
			# or by Funtoo itself for updating official kits and meta-repo.
			self.push = push
			self.nest_kits = False
			self.push = push
			self.mirror_repos = push
			self.git_class = GitTree

		meta_repo_config = self.release_yaml.get_meta_repo_config()
		self.meta_repo = self.git_class(
			name="meta-repo",
			branch=release,
			url=meta_repo_config['url'],
			root=self.dest_trees + "/meta-repo",
			origin_check=True if self.prod else None,
			mirrors=meta_repo_config['mirrors'],
			create_branches=self.create_branches,
			model=self
		)
		self.start_time = datetime.utcnow()
		self.meta_repo.initialize()

	def get_package_data(self, ctx):
		key = f"{ctx.kit.name}/{ctx.kit.branch}"
		if key not in self._package_data_dict:
			# Try to use branch-specific packages.yaml if it exists. Fall back to global kit-specific YAML:
			fn = f"{self.kit_fixups.root}/{key}/packages.yaml"
			if not os.path.exists(fn):
				fn = f"{self.kit_fixups.root}/{ctx.kit.name}/packages.yaml"
			with open(fn, "r") as f:
				self._package_data_dict[key] = yaml.safe_load(f)
		return self._package_data_dict[key]

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

	def get_kit_items(self, ctx, section="packages"):
		pdata = self.get_package_data(ctx)
		if section in pdata:
			for package_set in pdata[section]:
				repo_name = list(package_set.keys())[0]
				if section == "packages":
					# for packages, allow arbitrary nesting, only capturing leaf nodes (catpkgs):
					yield repo_name, self.yaml_walk(package_set)
				else:
					# not a packages section, and just return the raw YAML subsection for further parsing:
					packages = package_set[repo_name]
					yield repo_name, packages

	def get_kit_packages(self, ctx):
		return self.get_kit_items(ctx)

	def get_excludes(self, ctx):
		"""
		Grabs the excludes: section from packages.yaml, which is used to remove stuff from the resultant
		kit that accidentally got copied by merge scripts (due to a directory looking like an ebuild
		directory, for example.)
		"""
		pdata = self.get_package_data(ctx)
		if "exclude" in pdata:
			return pdata["exclude"]
		else:
			return []

	def get_copyfiles(self, ctx):
		"""
		Parses the 'eclasses' and 'copyfiles' sections in a kit's YAML and returns a list of files to
		copy from each source repository in a tuple format.
		"""
		eclass_items = list(self.get_kit_items(ctx, section="eclasses"))
		copyfile_items = list(self.get_kit_items(ctx, section="copyfiles"))
		copy_tuple_dict = defaultdict(list)

		for src_repo, eclasses in eclass_items:
			for eclass in eclasses:
				copy_tuple_dict[src_repo].append((f"eclass/{eclass}.eclass", f"eclass/{eclass}.eclass"))

		for src_repo, copyfiles in copyfile_items:
			for copy_dict in copyfiles:
				copy_tuple_dict[src_repo].append((copy_dict["src"], copy_dict["dest"] if "dest" in copy_dict else copy_dict["src"]))
		return copy_tuple_dict

	@property
	def mirror_url(self):
		return self.get_option("urls", "mirror", default=False)

	@property
	def gentoo_staging(self):
		return self.get_option("sources", "gentoo-staging")

	def url(self, repo, kind="auto"):
		base = self.get_option("urls", kind)
		if not base.endswith("/"):
			base += "/"
		if not repo.endswith(".git"):
			repo += ".git"
		return base + repo

	def branch(self, key):
		return self.get_option("branches", key, default="master")

	@property
	def metadata_cache(self):
		return os.path.join(self.work_path, "metadata-cache")

	@property
	def source_trees(self):
		return os.path.join(self.work_path, "source-trees")

	@property
	def dest_trees(self):
		return os.path.join(self.work_path, "dest-trees")


