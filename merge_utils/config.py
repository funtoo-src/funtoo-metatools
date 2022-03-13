#!/usr/bin/python3

import os
import sys
from configparser import ConfigParser

class Configuration:


	def __init__(self, prod=False, path=None, job=None, fastpull=None):
		self.prod = prod
		self.job = job
		self.fastpull = fastpull
		if path is None:
			home_dir = os.path.expanduser("~")
			self.config_path = os.path.join(home_dir, ".merge")
		else:
			self.config_path = path
		if not prod:
			self.defaults = {
				"urls": {
					"auto": "https://code.funtoo.org/bitbucket/scm/auto",
					"indy": "https://code.funtoo.org/bitbucket/scm/indy",
					"mirror": "",
				},
				"sources": {
					"flora": "https://code.funtoo.org/bitbucket/scm/co/flora.git",
					"kit-fixups": "https://code.funtoo.org/bitbucket/scm/core/kit-fixups.git",
					"gentoo-staging": "https://code.funtoo.org/bitbucket/scm/auto/gentoo-staging.git",
				},
			}
		else:
			self.defaults = {
				"urls": {
					"auto": "ssh://git@code.funtoo.org:7999/auto",
					"indy": "ssh://git@code.funtoo.org:7999/indy",
					"mirror": "git@github.com:funtoo",
				},
				"sources": {
					"flora": "ssh://git@code.funtoo.org:7999/co/flora.git",
					"kit-fixups": "ssh://git@code.funtoo.org:7999/core/kit-fixups.git",
					"gentoo-staging": "ssh://git@code.funtoo.org:7999/auto/gentoo-staging.git",
				},
			}
		self.config = ConfigParser()
		if os.path.exists(self.config_path):
			self.config.read(self.config_path)

		valids = {
			"main": ["features"],
			"paths": ["fastpull"],
			"sources": ["flora", "kit-fixups", "gentoo-staging"],
			"destinations": ["base_url", "mirror", "indy_url"],
			"branches": ["flora", "kit-fixups", "meta-repo"],
			"work": ["source", "destination", "metadata-cache"],
		}
		for section, my_valids in valids.items():

			if self.config.has_section(section):
				if section == "database":
					continue
				for opt in self.config[section]:
					if opt not in my_valids:
						print("Error: ~/.merge [%s] option %s is invalid." % (section, opt))
						sys.exit(1)
		print(f"Fastpull enabled: {self.fastpull_enabled}")

	def get_option(self, section, key, default=None):
		if self.config.has_section(section) and key in self.config[section]:
			my_path = self.config[section][key]
		elif section in self.defaults and key in self.defaults[section]:
			my_path = self.defaults[section][key]
		else:
			my_path = default
		return my_path

	@property
	def flora(self):
		return self.get_option("sources", "flora")

	@property
	def kit_fixups(self):
		return self.get_option("sources", "kit-fixups")

	@property
	def meta_repo(self):
		return self.url("meta-repo")

	@property
	def mirror(self):
		return self.get_option("urls", "mirror", default=False)

	@property
	def destination_base_url(self):
		return self.get_option("destinations", "base_url", default=None)

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
	def work_path(self):
		if "HOME" in os.environ:
			return os.path.join(os.environ["HOME"], "repo_tmp")
		else:
			return "/var/tmp/repo_tmp"

	@property
	def temp_path(self):
		"""
		merge-kits may run multiple 'doit's in parallel. In this case, we probably want to segregate their temp
		paths. We can do this by having a special option passed to doit which can in turn tweak the Configuration
		object to create unique sub-paths here.
		This is TODO item!
		"""
		if "HOME" in os.environ:
			return os.path.join(os.environ["HOME"], "repo_tmp/tmp")
		else:
			return "/var/tmp/repo_tmp/tmp"

	@property
	def fastpull_path(self):
		"""
		In theory, multiple fastpull hooks could try to link the same file into the same fastpull location at
		the same time resulting in a code failure.

		Possibly, we could have a 'staging' fastpull for each 'doit' call, and the master merge-kits process
		could look in this area and move files into its main fastpull db from its main process rather than
		relying on each 'doit' process to take care of it.

		Maybe this only happens when 'doit' is run as part of merge-kits. When run separately, 'doit' would
		populate the main fastpull db itself.

		In any case, some resiliency in the code for multiple creation of the same symlink (and thus symlink
		creation failure) would be a good idea.

		"""
		fp_path = self.get_option("paths", "fastpull")
		if fp_path is None:
			return os.path.join(self.work_path, "fastpull")
		return fp_path

	@property
	def metadata_cache(self):
		return os.path.join(self.work_path, "metadata-cache")

	@property
	def source_trees(self):
		return os.path.join(self.work_path, "source-trees")

	@property
	def dest_trees(self):
		return os.path.join(self.work_path, "dest-trees")

	@property
	def kit_dest(self):
		if self.prod:
			return self.dest_trees
		else:
			return os.path.join(self.dest_trees, "meta-repo/kits")

	@property
	def fastpull_enabled(self):
		# If set via constructor, we use that. Otherwise, read from config.
		if self.fastpull is not None:
			return self.fastpull
		features = self.get_option("main", "features", "")
		f_split = features.split()
		if "fastpull" in f_split:
			return True
		else:
			return False

	@property
	def fetch_download_path(self):
		if self.job:
			return os.path.join(self.work_path, "fetch", self.job)
		return os.path.join(self.work_path, "fetch")
