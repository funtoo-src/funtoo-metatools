import logging
import os
import sys
from collections import defaultdict
from datetime import timedelta

import pymongo
import yaml

from metatools.config.base import MinimalConfig
from metatools.config.mongodb import get_collection
from metatools.fastpull.blos import BaseLayerObjectStore
from metatools.fastpull.core import FastPullIntegrityDatabase
from metatools.fastpull.spider import WebSpider
from subpop.config import ConfigurationError


class Tree:
	def __init__(self, root=None, start=None):
		self.root = root
		self.start = start


def fetch_cache():
	fc = get_collection('fetch_cache')
	fc.create_index([("method_name", pymongo.ASCENDING), ("url", pymongo.ASCENDING)])
	fc.create_index("last_failure_on", partialFilterExpression={"last_failure_on": {"$exists": True}})
	return fc


class AutogenConfig(MinimalConfig):
	"""
	This class is used for the autogen workflow -- i.e. the 'doit' command.
	"""
	fetch_cache = fetch_cache()
	fetch_cache_interval = timedelta(minutes=15)
	check_disk_hashes = False
	manifest_lines = defaultdict(set)
	fetch_attempts = 3
	context = None
	output_context = None
	start_path = None
	out_path = None
	config = None
	kit_spy = None
	spider = None
	fpos = None
	fastpull_scope = None
	fastpull_session = None
	hashes = None

	config_files = {
		"autogen": "~/.autogen"
	}

	async def initialize(self, start_path=None, out_path=None, fetch_cache_interval=None, fastpull_scope=None):
		self.fastpull_scope = fastpull_scope
		if fetch_cache_interval:
			# use our default unless another timedelta specified:
			self.fetch_cache_interval = fetch_cache_interval
		self.start_path = start_path
		self.out_path = out_path
		self.kit_spy = None
		self.config = yaml.safe_load(self.get_file("autogen"))
		self.set_context()
		self.hashes = {'sha512', 'size', 'blake2b', 'sha256'}
		self.blos = BaseLayerObjectStore(self.fastpull_path)
		self.spider = WebSpider(os.path.join(self.temp_path, "spider"), hashes=self.hashes)
		self.fpos = FastPullIntegrityDatabase(
			blos=self.blos,
			spider=self.spider,
			hashes=self.hashes
		)
		self.fastpull_session = self.fpos.get_scope(self.fastpull_scope)
		sys.stdout.write(f"fetch cache interval set to {self.fetch_cache_interval}\n")

	def repository_of(self, start_path):
		root_path = start_path
		while (
				root_path != "/"
				and not os.path.exists(os.path.join(root_path, "profiles/repo_name"))
				and not os.path.exists(os.path.join(root_path, "metadata/layout.conf"))
		):
			root_path = os.path.dirname(root_path)
		if root_path == "/":
			return None

		repo_name = None
		repo_name_path = os.path.join(root_path, "profiles/repo_name")
		if os.path.exists(repo_name_path):
			with open(repo_name_path, "r") as repof:
				repo_name = repof.read().strip()

		if repo_name is None:
			logging.warning("Unable to find %s." % repo_name_path)

		return Tree(root=root_path, start=start_path)

	def set_context(self):
		self.context = self.repository_of(self.start_path)
		if self.out_path is None or self.start_path == self.out_path:
			self.output_context = self.context
		else:
			self.output_context = self.repository_of(self.out_path)
		if self.context is None:
			raise ConfigurationError(
				"Could not determine repo context: %s -- please create a profiles/repo_name file in your repository." % self.start_path
			)
		elif self.output_context is None:
			raise ConfigurationError(
				"Could not determine output repo context: %s -- please create a profiles/repo_name file in your repository."
				% self.out_path
			)
		self.kit_spy = "/".join(self.context.root.split("/")[-2:])
		logging.debug("Set source context to %s." % self.context.root)
		logging.debug("Set output context to %s." % self.output_context.root)

	#model.CHECK_DISK_HASHES = False
	#model.AUTOGEN_CONFIG = load_autogen_config()
	#model.MANIFEST_LINES = defaultdict(set)
	# This is used to limit simultaneous connections to a particular hostname to a reasonable value.
	#model.FETCH_ATTEMPTS = 3
