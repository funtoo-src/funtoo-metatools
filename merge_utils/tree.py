import asyncio
import glob
import os
import subprocess
import sys

debug = True


def run(args, env=None):
	if env:
		result = subprocess.run(args, shell=True, env=env, capture_output=True, encoding="utf-8")
	else:
		result = subprocess.run(args, shell=True, capture_output=True, encoding="utf-8")
	return result


def runShell(cmd_list, abort_on_failure=True, env=None):
	if debug:
		print("running: %r" % cmd_list)
	if isinstance(cmd_list, list):
		cmd_str = " ".join(cmd_list)
	else:
		cmd_str = cmd_list
	result: subprocess.CompletedProcess = run(cmd_str, env=env)
	if result.returncode != 0:
		print("Error executing %r" % cmd_str)
		print()
		print("output:")
		print(result.stdout)
		if abort_on_failure:
			sys.exit(1)
		else:
			return False
	return True


def headSHA1(tree):
	retval, out = subprocess.getstatusoutput("(cd %s && git rev-parse HEAD)" % tree)
	if retval == 0:
		return out.strip()
	return None


class Tree:
	def __init__(self, hub, root=None):
		self.hub = hub
		self.root = root
		self.autogenned = False
		self.name = None
		self.merged = []

	def logTree(self, srctree):
		# record name and SHA of src tree in dest tree, used for git commit message/auditing:
		if srctree.name is None:
			# this tree doesn't have a name, so just copy any existing history from that tree
			self.merged.extend(srctree.merged)
		else:
			# this tree has a name, so record the name of the tree and its SHA1 for reference
			if hasattr(srctree, "origroot"):
				self.merged.append([srctree.name, headSHA1(srctree.origroot)])
				return
			self.merged.append([srctree.name, srctree.head()])

	@property
	def should_autogen(self):
		return self.name == "kit-fixups"

	async def autogen(self, src_offset=None):
		if src_offset is None:
			src_offset = ""
		if self.autogenned == src_offset:
			return
		autogen_path = os.path.join(self.root, src_offset)
		if not os.path.exists(autogen_path):
			print("Skipping autogen as src_offset %s (in %s) doesn't exist!" % (src_offset, autogen_path))
			return
		print(f"Starting autogen {self.hub.AUTOGEN_OPTS} in src_offset {src_offset} (in {autogen_path})...")
		# use subprocess.call so we can see the output of autogen:
		retcode = subprocess.call(f"(cd {autogen_path} && doit {self.hub.AUTOGEN_OPTS})", shell=True)
		if retcode != 0:
			raise GitTreeError("Autogen failed.")
		self.autogenned = src_offset

	async def cleanTree(self):
		print("Cleaning tree %s" % self.root)
		runShell("(cd %s &&  git reset --hard && git clean -fd )" % self.root)
		self.autogenned = False

	def getDepthOfCommit(self, sha1):
		s, depth = subprocess.getstatusoutput("( cd %s && git rev-list HEAD ^%s --count)" % (self.root, sha1))
		return int(depth) + 1

	def localBranchExists(self, branch):
		s, branch = subprocess.getstatusoutput(
			"( cd %s && git show-ref --verify --quiet refs/heads/%s )" % (self.root, branch)
		)
		if s:
			return False
		else:
			return True

	async def run(self, steps):
		for step in steps:
			if step is not None:
				print("Running step", step.__class__.__name__, self.root)
				await step.run(self)

	def head(self):
		return headSHA1(self.root)

	@property
	def currentLocalBranch(self):
		s, branch = subprocess.getstatusoutput("( cd %s && git symbolic-ref --short -q HEAD )" % self.root)
		if s:
			return None
		else:
			return branch

	async def initialize(self):
		if not self.initialized:
			await self._initialize_tree()

	async def gitCheckout(self, branch=None, from_init=False):
		if not from_init:
			await self.initialize()
		if self.currentLocalBranch != branch:
			if self.localBranchExists(branch):
				runShell("(cd %s && git checkout %s)" % (self.root, branch))
			else:
				# An AutoCreatedGitTree will automatically create branches as needed, as forks of master.
				runShell("(cd %s && git checkout master && git checkout -b %s)" % (self.root, branch))
			await self.cleanTree()
		if self.currentLocalBranch != branch:
			raise GitTreeError(
				"%s: On branch %s. not able to check out branch %s." % (self.root, self.currentLocalBranch, branch)
			)
		print("Checked out %s on tree %s" % (branch, self.root))
		self.branch = branch


class GitTreeError(Exception):
	pass


class AutoCreatedGitTree(Tree):
	"""
	This is a locally-created Git Tree, typically used for local development purposes. Tree will be created
	if it doesn't exist. It doesn't support remotes. It will not push, or fetch. It's your basic "create a
	temporary local git tree to put stuff in because I'm too lazy to use a real existing git tree or am testing
	stuff"-type tree.
	"""

	def __init__(self, hub, name: str, branch: str = "master", root: str = None, commit_sha1: str = None, **kwargs):
		super().__init__(hub, root=root)
		self.branch = branch
		self.name = self.reponame = name
		self.has_cleaned = False
		self.initialized = False
		self.commit_sha1 = commit_sha1
		self.do_fetch = False
		self.merged = []

	async def gitCommit(self, message="", skip=None, push=False):
		"""

		:param message:
		:param skip: Files or dirs in the root directory of the path to purposefully *not* git add.
		:param push: This argument is ignored in this implementation.
		:return:
		"""
		if skip is None:
			skip = []
		for x in os.listdir(self.root):
			if x not in skip:
				runShell("( cd %s && git add %s )" % (self.root, x))
		cmd = '( cd %s && [ -n "$(git status --porcelain)" ] && git commit -a -F - << EOF\n' % self.root
		if message != "":
			cmd += "%s\n\n" % message
		names = []
		if len(self.merged):
			cmd += "merged: \n\n"
			for name, sha1 in self.merged:
				if name in names:
					# don't print dups
					continue
				names.append(name)
				if sha1 is not None:
					cmd += "  %s: %s\n" % (name, sha1)
		cmd += "EOF\n"
		cmd += ")\n"
		print("running: %s" % cmd)
		# we use os.system because this multi-line command breaks runShell() - really, breaks commands.getstatusoutput().
		myenv = os.environ.copy()
		if os.geteuid() == 0:
			# make sure HOME is set if we are root (maybe we entered to a minimal environment -- this will mess git up.)
			# In particular, a new tmux window will have HOME set to /root but NOT exported. Which will mess git up. (It won't know where to find ~/.gitconfig.)
			myenv["HOME"] = "/root"
		cp = subprocess.run(cmd, shell=True, env=myenv)
		retval = cp.returncode
		if retval not in [0, 1]:  # can return 1
			print("retval is: %s" % retval)
			print(cp)
			print("Commit failed.")
			sys.exit(1)

	async def _initialize_tree(self):
		if not os.path.exists(self.root):
			os.makedirs(self.root)
			runShell("( cd %s && git init )" % self.root)
			runShell("echo 'created by merge.py' > %s/README" % self.root)
			runShell("( cd %s &&  git add README; git commit -a -m 'initial commit by merge.py' )" % self.root)
			if not self.localBranchExists(self.branch):
				runShell("( cd %s && git checkout -b %s)" % (self.root, self.branch))
			else:
				await self.gitCheckout(self.branch, from_init=True)

		if not self.has_cleaned:
			runShell("(cd %s &&  git reset --hard && git clean -fd )" % self.root)
			self.has_cleaned = True

		# point to specified sha1:

		if self.commit_sha1:
			runShell("(cd %s && git checkout %s )" % (self.root, self.commit_sha1))
			if self.head() != self.commit_sha1:
				raise GitTreeError("%s: Was not able to check out specified SHA1: %s." % (self.root, self.commit_sha1))
			if self.currentLocalBranch != self.branch:
				raise GitTreeError("Checking out of SHA1 resulted in switching branch to: %s. Aborting." % self.currentLocalBranch)
		self.initialized = True


class GitTree(Tree):
	"A Tree (git) that we can use as a source for work jobs, and/or a target for running jobs."

	def __init__(
		self,
		hub,
		name: str,
		branch: str = "master",
		url: str = None,
		commit_sha1: str = None,
		root: str = None,
		reponame: str = None,
		mirror: str = None,
		forcepush: bool = False,
		origin_check: bool = True,
		destfix: bool = False,
		reclone: bool = False,
	):

		# note that if create=True, we are in a special 'local create' mode which is good for testing. We create the repo locally from
		# scratch if it doesn't exist, as well as any branches. And we don't push.
		super().__init__(hub, root=root)

		self.name = name
		self.url = url
		self.merged = []
		self.pull = True
		# avoid pulling multiple times:
		self.pulled = False
		self.reponame = reponame
		self.has_cleaned = False
		self.initialized = False
		self.mirror = mirror
		self.origin_check = origin_check
		self.destfix = destfix
		self.reclone = reclone
		self.forcepush = "--force" if forcepush else "--no-force"
		self.branch = branch
		self.commit_sha1 = commit_sha1

	# if we don't specify root destination tree, assume we are source only:

	async def _initialize_tree(self):
		if self.root is None:
			base = self.hub.MERGE_CONFIG.source_trees
			self.root = "%s/%s" % (base, self.name)

		if os.path.isdir("%s/.git" % self.root) and self.reclone:
			runShell("rm -rf %s" % self.root)

		if not os.path.isdir("%s/.git" % self.root):
			# repo does not exist? - needs to be cloned or created
			if os.path.exists(self.root):
				raise GitTreeError("%s exists but does not appear to be a valid git repository." % self.root)

			base = os.path.dirname(self.root)
			if self.url:
				if not os.path.exists(base):
					os.makedirs(base)
				# we aren't supposed to create it from scratch -- can we clone it?
				runShell("(cd %s && git clone %s %s)" % (base, self.url, os.path.basename(self.root)))

			else:
				# we've run out of options
				print("Error: tree %s does not exist, but no clone URL specified. Exiting." % self.root)
				sys.exit(1)

		# create local tracking branches for all remote branches. - we want to do this for every initialization.
		s, o = subprocess.getstatusoutput("(cd %s && git branch -r | grep -v /HEAD)" % self.root)
		if s != 0:
			# if repo is totally uninitialized (like gitolite wildrepo) -- initialize it with a first commit.
			print("Attempting to initialize git repository for first use...")
			runShell(
				"(cd %s && touch README && git add README && git commit -a -m 'first commit' && git push %s)"
				% (self.root, self.forcepush)
			)
			s, o = subprocess.getstatusoutput("(cd %s && git branch -r | grep -v /HEAD)" % self.root)
			if s != 0:
				print("Error listing local branches.")
				sys.exit(1)
		for branch in o.split():
			branch = branch.split("/")[-1]
			if not self.localBranchExists(branch):
				runShell("( cd %s && git checkout %s)" % (self.root, branch))

		# if we've gotten here, we can assume that the repo exists at self.root.
		if self.url is not None and self.origin_check:
			result = run("(cd %s && git remote get-url origin)" % self.root)
			out = result.stdout
			my_url = self.url
			if my_url.endswith(".git"):
				my_url = my_url[:-4]
			if out.endswith(".git"):
				out = out[:-4]
			if out != my_url:
				if self.destfix is True:
					print("WARNING: fixing remote URL for origin to point to %s" % my_url)
					self.setRemoteURL("origin", my_url)
				elif self.destfix is False:
					print("Error: remote url for origin at %s is:" % self.root)
					print()
					print("  existing:", out)
					print("  expected:", self.url)
					print()
					print("Please fix or delete any repos that are cloned from the wrong origin.")
					print("To do this automatically, use the --destfix option with merge-all-kits.")
					raise GitTreeError("%s: Git origin mismatch." % self.root)
				elif self.destfix is None:
					pass
		# first, we will clean up any messes:
		if not self.has_cleaned:
			await self.cleanTree()
			self.has_cleaned = True

		# git fetch will run as part of this:
		await self.gitCheckout(self.branch, from_init=True)

		# point to specified sha1:

		if self.commit_sha1:
			runShell("(cd %s && git checkout %s )" % (self.root, self.commit_sha1))
			if self.head() != self.commit_sha1:
				raise GitTreeError("%s: Was not able to check out specified SHA1: %s." % (self.root, self.commit_sha1))
		elif self.pull:
			await self.do_pull()

		self.initialized = True

	async def do_pull(self):
		if not self.pulled:
			# we are on the right branch, but we want to make sure we have the latest updates
			runShell("(cd %s && git pull --no-force --all || true)" % self.root)
			self.pulled = True

	def getRemoteURL(self, remote):
		s, o = subprocess.getstatusoutput("( cd %s && git remote get-url %s )" % (self.root, remote))
		if s:
			return None
		else:
			return o.strip()

	def setRemoteURL(self, mirror_name, url):
		s, o = subprocess.getstatusoutput("( cd %s && git remote add %s %s )" % (self.root, mirror_name, url))
		if s:
			return False
		else:
			return True

	def remoteBranchExists(self, branch):
		s, o = subprocess.getstatusoutput("( cd %s && git show-branch remotes/origin/%s )" % (self.root, branch))
		if s:
			return False
		else:
			return True

	def getAllCatPkgs(self):
		cats = set()
		try:
			with open(self.root + "/profiles/categories", "r") as a:
				cats = set(a.read().split())
		except FileNotFoundError:
			pass
		for item in glob.glob(self.root + "/*-*"):
			if os.path.isdir(item):
				cat = os.path.basename(item)
				if cat not in cats:
					print("!!! WARNING: category %s not in categories... should be added to profiles/categories!" % item)
				cats.add(cat)
		cats = sorted(list(cats))
		catpkgs = {}

		for cat in cats:
			if not os.path.exists(self.root + "/" + cat):
				continue
			pkgs = os.listdir(self.root + "/" + cat)
			for pkg in pkgs:
				if not os.path.isdir(self.root + "/" + cat + "/" + pkg):
					continue
				catpkgs[cat + "/" + pkg] = self.name
		return catpkgs

	def catpkg_exists(self, catpkg):
		return os.path.exists(self.root + "/" + catpkg)

	async def gitCheckout(self, branch=None, sha1=None, from_init=False):
		"""
		TODO: this method currently isn't working correctly.
		New gitCheckout method that tries to avoid calling cleanTree() if possible, since that allows us to avoid
		re-autogenning in the tree.
		:param branch:
		:param from_init:
		:return:
		"""
		if branch is None and sha1 is None:
			raise GitTreeError("Please specify at least a branch or a sha1.")

		if not from_init:
			await self.initialize()
		if sha1 is not None and self.head() != sha1:
			runShell("(cd %s && git fetch --verbose && git checkout %s)" % (self.root, sha1))
			await self.cleanTree()
			if self.head() != sha1:
				raise GitTreeError("Not able to check out requested sha1: %s, got: %s" % (sha1, self.head()))
		else:
			if self.currentLocalBranch != branch:
				runShell("(cd %s && git fetch --verbose)" % self.root)
				if self.localBranchExists(branch):
					runShell("(cd %s && git checkout %s)" % (self.root, branch))
				elif self.remoteBranchExists(branch):
					# An AutoCreatedGitTree will automatically create branches as needed, as forks of master.
					runShell("(cd %s && git checkout -b %s --track origin/%s)" % (self.root, branch, branch))
				else:
					runShell("(cd %s && git checkout -b %s)" % (self.root, branch))
				await self.cleanTree()
			else:
				old_head = self.head()
				await self.do_pull()
				new_head = self.head()
				if old_head != new_head:
					await self.cleanTree()
		if branch and self.currentLocalBranch != branch:
			raise GitTreeError(
				"%s: On branch %s. not able to check out branch %s." % (self.root, self.currentLocalBranch, branch)
			)
		self.branch = branch

	async def gitCheckoutOld(self, branch="master", from_init=False):
		if not from_init:
			await self.initialize()
		runShell("(cd %s && git fetch --verbose)" % self.root)
		if self.localBranchExists(branch):
			await self.do_pull()
		elif self.remoteBranchExists(branch):
			runShell("(cd %s && git checkout -b %s --track origin/%s)" % (self.root, branch, branch))
		else:
			runShell("(cd %s && git checkout -b %s)" % (self.root, branch))
		await self.cleanTree()
		if self.currentLocalBranch != branch:
			raise GitTreeError(
				"%s: On branch %s. not able to check out branch %s." % (self.root, self.currentLocalBranch, branch)
			)

	async def mirrorLocalBranches(self, mirror=None):
		if mirror is None:
			mirror = self.url
		# This is a special push command that will push local tags and branches *only*
		runShell("(cd %s && git push %s %s +refs/heads/* +refs/tags/*)" % (self.root, self.forcepush, mirror))

	async def mirrorUpstreamRepository(self, mirror):
		# This is a special push command that will push all the stuff from origin (branches and tags) *only*
		# It will skip local branches.
		runShell("(cd %s && git fetch --prune)" % self.root)
		runShell(
			"(cd %s && git push %s --prune %s +refs/remotes/origin/*:refs/heads/* +refs/tags/*:refs/tags/*)"
			% (self.root, self.forcepush, mirror)
		)

	async def gitMirrorPush(self):
		runShell(
			"(cd %s && ( git rev-parse --abbrev-ref --symbolic-full-name @{u} || git branch --set-upstream-to origin/%s))"
			% (self.root, self.branch)
		)
		await self.mirrorLocalBranches()
		if self.mirror:
			await self.mirrorUpstreamRepository(self.mirror)

	async def gitCommit(self, message="", skip=None, push=True):
		if skip is None:
			skip = []
		for x in os.listdir(self.root):
			if x not in skip:
				runShell("( cd %s && git add %s )" % (self.root, x))
		cmd = '( cd %s && [ -n "$(git status --porcelain)" ] && git commit -a -F - << EOF\n' % self.root
		if message != "":
			cmd += "%s\n\n" % message
		names = []
		if len(self.merged):
			cmd += "merged: \n\n"
			for name, sha1 in self.merged:
				if name in names:
					# don't print dups
					continue
				names.append(name)
				if sha1 is not None:
					cmd += "  %s: %s\n" % (name, sha1)
		cmd += "EOF\n"
		cmd += ")\n"
		print("running: %s" % cmd)
		# we use os.system because this multi-line command breaks runShell() - really, breaks commands.getstatusoutput().
		myenv = os.environ.copy()
		if os.geteuid() == 0:
			# make sure HOME is set if we are root (maybe we entered to a minimal environment -- this will mess git up.)
			# In particular, a new tmux window will have HOME set to /root but NOT exported. Which will mess git up. (It won't know where to find ~/.gitconfig.)
			myenv["HOME"] = "/root"
		cp = subprocess.run(cmd, shell=True, env=myenv)
		retval = cp.returncode
		if retval not in [0, 1]:  # can return 1
			print("retval is: %s" % retval)
			print(cp)
			print("Commit failed.")
			sys.exit(1)
		if push is True:
			await self.mirrorLocalBranches()
			if self.mirror:
				await self.mirrorUpstreamRepository(mirror=self.mirror)
		else:
			print("Pushing disabled.")


class RsyncTree(Tree):
	def __init__(self, hub, name, url="rsync://rsync.us.gentoo.org/gentoo-portage/"):
		super().__init__(hub)
		self.name = name
		self.url = url
		base = self.hub.MERGE_CONFIG.source_trees
		self.root = "%s/%s" % (base, self.name)
		if not os.path.exists(base):
			os.makedirs(base)
		runShell(
			"rsync --recursive --delete-excluded --links --safe-links --perms --times --compress --force --whole-file --delete --timeout=180 --exclude=/.git --exclude=/metadata/cache/ --exclude=/metadata/glsa/glsa-200*.xml --exclude=/metadata/glsa/glsa-2010*.xml --exclude=/metadata/glsa/glsa-2011*.xml --exclude=/metadata/md5-cache/	--exclude=/distfiles --exclude=/local --exclude=/packages %s %s/"
			% (self.url, self.root)
		)
