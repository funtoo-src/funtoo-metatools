#!/usr/bin/python3
import atexit
import logging
import os

from rich.logging import RichHandler
from subpop.config import SubPopModel

from metatools.model import set_model


class MinimalConfig(SubPopModel):
	"""
	This class contains configuration settings common to all the metatools plugins and tools.
	"""

	logger_name = "metatools.merge"

	def __init__(self):
		super().__init__()

	async def initialize(self, debug=False):
		self.log = logging.getLogger(self.logger_name)
		self.log.propagate = False
		if debug:
			self.debug = debug
			self.log.setLevel(logging.DEBUG)
		else:
			self.log.setLevel(logging.INFO)
		handler = RichHandler(show_path=False, show_time=False)
		self.log.addHandler(handler)
		atexit.register(lambda: print("\x1b[?25h"))
		if debug:
			self.log.warning("DEBUG enabled")
		set_model(self.logger_name, self)

	@property
	def work_path(self):
		home = self.home()
		if home:
			return os.path.join(home, "repo_tmp")
		else:
			return "/var/tmp/repo_tmp"

	@property
	def moonbeam_socket(self):
		return os.path.join(self.temp_path, "moonbeam_socket")

	@property
	def source_trees(self):
		return os.path.join(self.work_path, "source-trees")

	@property
	def store_path(self):
		return os.path.join(self.work_path, "stores")

	@property
	def fetch_download_path(self):
		return os.path.join(self.work_path, "fetch")

	@property
	def temp_path(self):
		"""
		merge-kits may run multiple 'doit's in parallel. In this case, we probably want to segregate their temp
		paths. We can do this by having a special option passed to doit which can in turn tweak the Configuration
		object to create unique sub-paths here.
		This is TODO item!
		"""
		home = self.home()
		if home:
			return os.path.join(home, "repo_tmp/tmp")
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

		return os.path.join(self.work_path, "fastpull")

	@property
	def metadata_cache(self):
		return os.path.join(self.work_path, "metadata-cache")

	@property
	def dest_trees(self):
		return os.path.join(self.work_path, "dest-trees")





