import asyncio
import glob
import logging
import os
import subprocess


def head_sha1(tree):
	retval, out = subprocess.getstatusoutput("(cd %s && git rev-parse HEAD)" % tree)
	if retval == 0:
		return out.strip()
	return None


class Tree:

	def __init__(self, root=None, model=None):
		self.root = root
		self.autogenned = False
		self.name = None
		self.merged = []
		self.forcepush = "--no-force"
		self.url = None
		self.model = model
		self.initialized = False
		self.branch = None
		if self.model:
			self.log = self.model.log
		else:
			logging.basicConfig(level=logging.INFO)
			self.log = logging.getLogger()

	async def initialize(self):
		if not self.initialized:
			await self._initialize_tree()

	async def _initialize_tree(self):
		self.initialized = True

	def find_license(self, license):
		lic_path = f"{self.root}/licenses/{license}"
		if os.path.exists(lic_path):
			return lic_path
		else:
			raise FileNotFoundError(lic_path)

	async def run_shell(self, cmd_list, abort_on_failure=True, chdir=None):
		return await run_shell(cmd_list, abort_on_failure=abort_on_failure, chdir=chdir, logger=self.log)

	def log_tree(self, srctree):
		# record name and SHA of src tree in dest tree, used for git commit message/auditing:
		if srctree.name is None:
			# this tree doesn't have a name, so just copy any existing history from that tree
			self.merged.extend(srctree.merged)
		else:
			# this tree has a name, so record the name of the tree and its SHA1 for reference
			if hasattr(srctree, "origroot"):
				self.merged.append([srctree.name, head_sha1(srctree.origroot)])
				return
			self.merged.append([srctree.name, srctree.head()])

	async def autogen(self, src_offset=None, scope=None):
		if src_offset is None:
			src_offset = ""
		if self.autogenned == src_offset:
			return
		autogen_path = os.path.join(self.root, src_offset)
		if not os.path.exists(autogen_path):
			self.log.warning(f"Skipping autogen as src_offset {src_offset} (in {autogen_path}) doesn't exist!")
			return
		self.log.info(f"Starting autogen in src_offset {src_offset} (in {autogen_path})... (DEBUG={self.model.debug}) (orig_scope={scope})")
		if scope is None:
			scope = "local"
		self.log.info(f"Final scope: {scope}")
		cmd_str = f"cd {autogen_path} && doit --fast --fastpull_scope={scope} --moonbeam"
		if self.model.debug:
			cmd_str += " --debug"
			self.log.debug(cmd_str)
		if self.model.prod:
			cmd_str += " --prod"
		# We must fork and run this async, so we can receive moonbeam messages while this is running:
		retcode = await run_cmd_bg(cmd_str)
		# use subprocess.call so we can see the output of autogen:
		# TODO: we don't need to see this GitTreeError traceback
		if retcode != 0:
			self.log.error(f"Command failure from merge-kits: {cmd_str}")
			raise GitTreeError(f"failed autogen in {self.root} -- offset {src_offset}.")
		self.autogenned = src_offset

	async def clean_tree(self):
		self.log.info("Cleaning tree %s" % self.root)
		await self.run_shell(f"(cd {self.root} && git reset --hard && git clean -fdx )")
		self.autogenned = False

	def get_depth_of_commit(self, sha1):
		s, depth = subprocess.getstatusoutput("( cd %s && git rev-list HEAD ^%s --count)" % (self.root, sha1))
		return int(depth) + 1

	def local_branch_exists(self, branch):
		s, branch = subprocess.getstatusoutput(
			"( cd %s && git show-ref --verify --quiet refs/heads/%s )" % (self.root, branch)
		)
		if s:
			return False
		else:
			return True

	def head(self):
		return head_sha1(self.root)

	@property
	def current_local_branch(self):
		s, branch = subprocess.getstatusoutput("( cd %s && git symbolic-ref --short -q HEAD )" % self.root)
		if s:
			return None
		else:
			return branch

	async def git_checkout(self, branch=None, from_init=False):
		if not from_init:
			await self.initialize()
		await self.clean_tree()
		if self.current_local_branch != branch:
			if self.local_branch_exists(branch):
				await self.run_shell("(cd %s && git checkout %s)" % (self.root, branch))
			else:
				raise GitTreeError(f"Local branch {branch} does not exist but I was asked to checkout this branch.")
		if self.current_local_branch != branch:
			raise GitTreeError(f"{self.root}: On branch {self.current_local_branch}. Not able to check out branch {branch}.")
		self.branch = branch

	async def git_add(self, skip=None):
		if skip is None:
			skip = []
		skip.append(".git")
		files = ""
		for x in os.listdir(self.root):
			if x not in skip:
				files += " '" + x + "'"
		if files:
			await self.run_shell(f"cd {self.root} && git add {files[1:]}")

	async def git_commit(self, message="", skip=None, push=True):
		await self.git_add(skip=skip)
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
		myenv = os.environ.copy()
		if os.geteuid() == 0:
			# make sure HOME is set if we are root (maybe we entered to a minimal environment -- this will mess git up.)
			# In particular, a new tmux window will have HOME set to /root but NOT exported. Which will mess git up. (It won't know where to find ~/.gitconfig.)
			myenv["HOME"] = "/root"
		retval = await run_cmd_bg(cmd, env=myenv)
		if retval not in [0, 1]:  # can return 1
			print("Commit failed.")
			raise ShellError("Aborting due to failed command.")
		if push is True:
			await self.mirror_local_branches()

	async def mirror_local_branches(self):
		# This is a special push command that will push local tags and branches *only*
		await self.run_shell(f"(cd {self.root} && git push {self.forcepush} {self.url} +refs/heads/* +refs/tags/*)")


class GitTreeError(Exception):
	pass


class AutoCreatedGitTree(Tree):
	"""
	This is a locally-created Git Tree, typically used for local development purposes. Tree will be created
	if it doesn't exist. It doesn't support remotes. It will not push, or fetch. It's your basic "create a
	temporary local git tree to put stuff in because I'm too lazy to use a real existing git tree, or I'm testing
	stuff"-type tree.
	"""

	def __init__(self, name: str, branch: str = "master", root: str = None, commit_sha1: str = None, model=None, **kwargs):
		super().__init__(root=root, model=model)
		self.branch = branch
		self.name = self.reponame = name
		self.commit_sha1 = commit_sha1
		self.merged = []

	async def _create_branch(self):
		await run_shell(f"git checkout master; git checkout -b {self.branch}", chdir=self.root)

	async def _initialize_tree(self):
		if not os.path.exists(self.root):
			os.makedirs(self.root)
			await self.run_shell("( cd %s && git init )" % self.root)
			await self.run_shell("echo 'created by merge-kits.' > %s/README" % self.root)
			await self.run_shell("( cd %s &&	git add README; git commit -a -m 'initial commit by merge-kits' )" % self.root)

		if not self.local_branch_exists(self.branch):
			await self._create_branch()

		# This ensures we have the local branch active, cleans tree and performs sanity checks:
		await self.git_checkout(self.branch, from_init=True)

		# point to specified sha1:

		if self.commit_sha1:
			await self.run_shell("(cd %s && git checkout %s )" % (self.root, self.commit_sha1))
			if self.head() != self.commit_sha1:
				raise GitTreeError("%s: Was not able to check out specified SHA1: %s." % (self.root, self.commit_sha1))
			if self.current_local_branch != self.branch:
				raise GitTreeError("Checking out of SHA1 resulted in switching branch to: %s. Aborting." % self.current_local_branch)
		self.initialized = True


class GitTree(Tree):
	"""
		A Tree (git) that we can use as a source for work jobs, and/or a target for running jobs.
	"""

	def __init__(
			self,
			name: str,
			branch: str = None,
			# Set to True to only enforce the branch on initial clone, and interpret None to mean "keep existing
			# branch." This allows us to initialize kit-fixups to a developer's branch and only change it when the
			# developer explicitly specifies the branch. So it changes meaninng from None to "master" to "current
			# checked out branch".
			keep_branch: bool = False,
			url: str = None,
			commit_sha1: str = None,
			root: str = None,
			reponame: str = None,
			mirrors: list = None,
			forcepush: bool = False,
			origin_check: bool = False,
			create_branches: bool = False,
			destfix: bool = False,
			reclone: bool = False,
			pull: bool = True,
			checkout_all_branches: bool = False,
			model=None
	):

		super().__init__(root=root, model=model)

		self.name = name
		self.url = url
		self.merged = []
		self.pull = pull
		# avoid pulling multiple times:
		self.pulled = False
		self.reponame = reponame
		self.has_cleaned = False
		self.mirrors = mirrors if mirrors else []
		self.origin_check = origin_check
		self.create_branches = create_branches
		self.destfix = destfix
		self.reclone = reclone
		self.forcepush = "--force" if forcepush else "--no-force"
		self.commit_sha1 = commit_sha1
		self.checkout_all_branches = checkout_all_branches
		self.keep_branch = keep_branch
		if not self.keep_branch and branch is None:
			self.branch = "master"
		else:
			self.branch = branch
		self.log.info(f"GitTree(): {self.name} {self.url} {self.branch} {self.commit_sha1}")

	async def _create_branches(self):
		await self.run_shell(f"git checkout master; git checkout -b {self.branch}", chdir=self.root)
		await self.run_shell(f"git push --set-upstream origin {self.branch}", chdir=self.root)

	# if we don't specify root destination tree, assume we are source only:

	async def has_local_changes(self):
		proc, out = await run_bg(f"(cd {self.root} && git status --porcelain)")
		out = out.strip()
		return len(out) > 0

	async def _initialize_tree(self):
		if self.root is None:
			base = self.model.source_trees
			self.root = "%s/%s" % (base, self.name)

		if os.path.isdir("%s/.git" % self.root) and self.reclone:
			await self.run_shell("rm -rf %s" % self.root)

		if not os.path.isdir("%s/.git" % self.root):
			if os.path.exists(self.root):
				raise GitTreeError("%s exists but does not appear to be a valid git repository." % self.root)

			base = os.path.dirname(self.root)
			if self.url:
				if not os.path.exists(base):
					os.makedirs(base)
				# we aren't supposed to create it from scratch -- can we clone it?
				await self.run_shell("(cd %s && git clone %s %s)" % (base, self.url, os.path.basename(self.root)))

			else:
				# we've run out of options
				print("Error: tree %s does not exist, but no clone URL specified. Exiting." % self.root)
				raise ShellError("Aborted due to failed command.")

		if await self.has_local_changes():
			await self.clean_tree()

		init_branches = []

		cur_branch = self.current_local_branch

		# We allow this to be turned off for GitTrees like kit-fixups, where you really don't need any branches
		# for mirroring purposes and there may be many bugfix branches:
		if self.checkout_all_branches:
			# We should also, for the sake of mirroring working, create all local branches for remote branches.
			proc, stdout = await run_bg(f"(cd {self.root} && git branch -r | grep -v /HEAD)")
			if proc.returncode != 0:
				# This will happen if, for example, meta-repo is an AutoGeneratedGitTree, and then it is referenced
				# as a regular GitTree by deepdive. It will have no remotes.
				init_branches.append(self.branch)
			else:
				for branch in stdout.split():
					init_branches.append("/".join(branch.split("/")[1:]))
				if self.branch not in init_branches:
					if self.create_branches:
						await self._create_branches()
						init_branches = [self.branch]
					else:
						raise ShellError(f"Could not find remote branch: {self.branch} in git tree {self.root}.")
				# Put the branch we want at the end, so we end up with it active/
				else:
					init_branches.remove(self.branch)
					init_branches += [self.branch]
		else:
			if self.branch is not None:
				init_branches.append(self.branch)

		for branch in init_branches:
			if not self.local_branch_exists(branch):
				await self.git_checkout(branch, from_init=True)

		# if we've gotten here, we can assume that the repo exists at self.root.
		if self.url is not None and self.origin_check:
			proc, out = await run_bg("(cd %s && git remote get-url origin)" % self.root)
			out = out.strip()
			my_url = self.url
			if my_url.endswith(".git"):
				my_url = my_url[:-4]
			if out.endswith(".git"):
				out = out[:-4]
			if out != my_url:
				if self.destfix is True:
					print("WARNING: fixing remote URL for origin to point to %s" % my_url)
					self.set_remote_url("origin", my_url)
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

		if self.keep_branch and self.branch is None and cur_branch is not None:
			await self.git_checkout(cur_branch, from_init=True)
		elif self.commit_sha1:
			await self.run_shell("(cd %s && git checkout %s )" % (self.root, self.commit_sha1))
			if self.head() != self.commit_sha1:
				raise GitTreeError("%s: Was not able to check out specified SHA1: %s." % (self.root, self.commit_sha1))
		else:
			await self.git_checkout(self.branch, from_init=True)

			await self.do_pull()
		self.initialized = True

	async def do_pull(self):
		if self.pull and not self.pulled:
			# we are on the right branch, but we want to make sure we have the latest updates
			await self.run_shell("(cd %s && git pull --ff-only)" % self.root)
			self.pulled = True

	def get_remote_url(self, remote):
		s, o = subprocess.getstatusoutput("( cd %s && git remote get-url %s )" % (self.root, remote))
		if s:
			return None
		else:
			return o.strip()

	def set_remote_url(self, mirror_name, url):
		s, o = subprocess.getstatusoutput("( cd %s && git remote add %s %s )" % (self.root, mirror_name, url))
		if s:
			return False
		else:
			return True

	def remote_branch_exists(self, branch):
		s, o = subprocess.getstatusoutput("( cd %s && git show-branch remotes/origin/%s )" % (self.root, branch))
		if s:
			return False
		else:
			return True

	def get_all_cat_pkgs(self):
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

	async def git_checkout(self, branch=None, sha1=None, from_init=False):
		"""
		New git_checkout method that tries to avoid calling cleanTree() if possible, since that allows us to avoid
		re-autogenning in the tree.
		"""
		self.model.log.info(f"gitCheckout: br: {branch} sha1: {sha1} init: {from_init}")
		if branch is None and sha1 is None:
			raise GitTreeError(f"Please specify at least a branch or a sha1. {self.name} {self.root}")

		if not from_init:
			await self.initialize()
		if sha1 is not None and self.head() != sha1:
			await self.run_shell("(cd %s && git fetch --verbose && git checkout %s)" % (self.root, sha1))
			await self.clean_tree()
			if self.head() != sha1:
				raise GitTreeError("Not able to check out requested sha1: %s, got: %s" % (sha1, self.head()))
		else:
			if self.current_local_branch != branch:
				await self.run_shell("(cd %s && git fetch --verbose)" % self.root)
				if self.local_branch_exists(branch):
					await self.run_shell("(cd %s && git checkout %s)" % (self.root, branch))
					await self.clean_tree()
					await self.do_pull()
				elif self.remote_branch_exists(branch):
					# An AutoCreatedGitTree will automatically create branches as needed, as forks of master.
					await self.run_shell("(cd %s && git checkout -b %s --track origin/%s)" % (self.root, branch, branch))
					await self.clean_tree()
					await self.do_pull()
				elif self.create_branches:
					await self.run_shell(f"(cd {self.root} && git checkout -b {branch})")
					await self.run_shell(f"echo 'created by merge-kits.' > {self.root}/README")
					await self.run_shell(f"(cd {self.root} && git add README; git commit -a -m 'initial commit by merge-kits' )")
					await self.run_shell(f"(cd {self.root} && git push --set-upstream origin {branch})")
					await self.clean_tree()
			else:
				old_head = self.head()
				await self.do_pull()
				new_head = self.head()
				if old_head != new_head:
					await self.clean_tree()
		if branch and self.current_local_branch != branch:
			raise GitTreeError(
				"%s: On branch %s. not able to check out branch %s." % (self.root, self.current_local_branch, branch)
			)
		self.branch = branch
		self.commit_sha1 = sha1


async def run_bg(cmd):
	"""
	Run command in a forked background process, await its completion -- don't emit any output, but instead capture
	stdout and err into a combined string.

	Return the process object and the combined string of stdout and stderr.
	"""
	proc = await asyncio.create_subprocess_shell(cmd,
	stdout=asyncio.subprocess.PIPE,
	stderr=asyncio.subprocess.STDOUT)

	stdout, stderr = await proc.communicate()
	return proc, stdout.decode("utf-8")


async def run_cmd_bg(cmd, env=None):
	"""
	Run command in a forked background process, await its completion -- output all its output to existing stdout/err.
	Return its returncode.
	"""
	proc = await asyncio.create_subprocess_shell(cmd, env=env)
	return await proc.wait()


class ShellError(Exception):
	pass


async def run_shell(cmd_list, abort_on_failure=True, chdir=None, logger=None):
	if isinstance(cmd_list, list):
		cmd_str = " ".join(cmd_list)
	else:
		cmd_str = cmd_list
	if logger:
		logger.info(f"executing: {cmd_str}")
	if chdir:
		cmd_str = f"( cd {chdir}; {cmd_str})"

	proc, output = await run_bg(cmd_str)

	if proc.returncode != 0:
		if abort_on_failure:
			raise ShellError(f"Aborted due to failed command. Error executing '{cmd_str}':\n{output}\n")
		else:
			return False
	return True
