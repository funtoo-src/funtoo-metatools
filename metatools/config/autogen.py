import os
from collections import defaultdict

import yaml

from metatools.blos import BaseLayerObjectStore
from metatools.config.base import MinimalConfig
from metatools.context import OverlayLocator, GitRepositoryLocator
from metatools.fastpull.core import IntegrityDatabase
from metatools.fastpull.spider import WebSpider
from metatools.fetch_cache import FileStoreFetchCache
from metatools.tree import GitTree


class StoreConfig(MinimalConfig):
	"""
	This is used by the fastpull-daemon for access to the stores, and as a base class for AutogenConfig (used
	by the 'doit' command).
	"""

	fpos = None
	fastpull_scope = None
	fastpull_session = None

	hashes = None
	blos = None
	debug = False
	log = None
	logger_name = "metatools.cdn"

	async def initialize(self, fastpull_scope=None, debug=False):
		await super().initialize(debug=debug)
		self.fastpull_scope = fastpull_scope

		self.hashes = {'sha512', 'size', 'blake2b', 'sha256'}
		self.blos = BaseLayerObjectStore(db_base_path=self.store_path, hashes=self.hashes)
		self.fpos = IntegrityDatabase(
			db_base_path=self.store_path,
			blos=self.blos,
			spider=self.spider,
			hashes=self.hashes
		)
		self.fastpull_session = self.fpos.get_scope(self.fastpull_scope)


class StoreSpiderConfig(StoreConfig):

	spider = None
	logger_name = 'metatools.spider'

	async def initialize(self, fastpull_scope=None, debug=False):
		await super().initialize(fastpull_scope=fastpull_scope, debug=debug)
		self.spider = WebSpider(os.path.join(self.temp_path, "spider"), hashes=self.hashes)
		# This turns on periodic logging of active downloads (to get rid of 'dots')
		await self.spider.start_asyncio_tasks()



class AutogenConfig(StoreSpiderConfig):
	"""
	This class is used for the autogen workflow -- i.e. the 'doit' command.
	"""

	fetch_cache = None
	fetch_cache_interval = None
	manifest_lines = defaultdict(set)
	fetch_attempts = 3
	config = None
	kit_spy = None
	kit_fixups = None
	filter = None
	filter_cat = None
	filter_pkg = None
	autogens = None
	logger_name = 'metatools.autogen'

	config_files = {
		"autogen": "~/.autogen"
	}

	@property
	def kit_spy(self):
		"""
		kit_spy is used for creating an autogen ID::
		 	task_args["autogen_id"] = f"{pkgtools.model.kit_spy}:{task_args['gen_path'][len(base)+1:]}"
		The autogen_id is intended to be used in the distfile integrity database, to tell use which autogen
		referenced the artifact, in the situation where we don't have a specific BreezyBuild. This was a recent
		add and may not be fully implemented or make sense based on our current architecture -- needs review
		so TODO
		"""
		return "/".join(self.locator.root.split("/")[-2:])

	async def initialize(self, fetch_cache_interval=None, fastpull_scope=None, debug=False, fixups_url=None, fixups_branch=None, fast=None, cat=None, pkg=None, autogens=None):
		await super().initialize(fastpull_scope=fastpull_scope, debug=debug)

		self.fetch_cache = FileStoreFetchCache(db_base_path=self.store_path)

		# Process specified autogens instead of recursing:
		self.autogens = autogens

		# Selective filtering of autogens:
		self.filter_cat = cat
		self.filter_pkg = pkg
		if self.filter_cat or self.filter_pkg:
			self.filter = True

		self.config = yaml.safe_load(self.get_file("autogen"))
		# Set to empty values if non-existent:
		if self.config is None:
			self.config = {}

		self.locator = OverlayLocator()
		self.current_repo = GitRepositoryLocator()
		current_repo_name = self.current_repo.root.split("/")[-1]
		if current_repo_name.startswith("kit-fixups"):
			self.kit_fixups_repo = self.current_repo
		else:
			# We are likely running autogen in a non-kit-fixups kit (like foo-kit-sources.)
			# We require a locally-available kit-fixups repo to access generators in kit-fixups.

			kit_fixups_root = os.path.join(self.source_trees, "kit-fixups")
			if not fast:
				self.log.info("Cloning/updating kit-fixups to access generators (--fast to use as-is)")
				self.kit_fixups = GitTree(
					name='kit-fixups',
					root=kit_fixups_root,
					model=self,
					url=fixups_url,
					branch=fixups_branch,
					keep_branch=True
				)
				self.kit_fixups.initialize()
			else:
				self.log.info(f"Generators will be sourced from {kit_fixups_root}")
			self.kit_fixups_repo = GitRepositoryLocator(start_path=kit_fixups_root)

		if fetch_cache_interval is not None:
			# use our default unless another timedelta specified:
			self.fetch_cache_interval = fetch_cache_interval

		repo_name = None
		repo_name_path = os.path.join(self.locator.root, "profiles/repo_name")
		if os.path.exists(repo_name_path):
			with open(repo_name_path, "r") as repof:
				repo_name = repof.read().strip()
		if repo_name is None:
			self.log.warning("Unable to find %s." % repo_name_path)
