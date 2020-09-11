#!/usr/bin/python3
import grp
import itertools
import multiprocessing
import os
import re
import shutil
import sys

from merge.portage import getAllEclasses
from merge.tree import runShell, GitTree


class MergeStep:

	# This is only used for Repository Steps:
	collector = None

	async def run(self, tree):
		pass


class ThirdPartyMirrors(MergeStep):
	"Add funtoo's distfiles mirror, and add funtoo's mirrors as gentoo back-ups."

	async def run(self, tree):
		orig = "%s/profiles/thirdpartymirrors" % tree.root
		new = "%s/profiles/thirdpartymirrors.new" % tree.root
		mirrors = "https://fastpull-us.funtoo.org/distfiles"
		a = open(orig, "r")
		b = open(new, "w")
		for line in a:
			ls = line.split()
			if len(ls) and ls[0] == "gentoo":
				b.write("gentoo\t" + ls[1] + " " + mirrors + " " + " ".join(ls[2:]) + "\n")
			else:
				b.write(line)
		b.write("funtoo %s\n" % mirrors)
		a.close()
		b.close()
		os.unlink(orig)
		os.link(new, orig)
		os.unlink(new)


class SyncDir(MergeStep):
	def __init__(self, srcroot, srcdir=None, destdir=None, exclude=None, delete=False):
		self.srcroot = srcroot
		self.srcdir = srcdir
		self.destdir = destdir
		self.exclude = exclude if exclude is not None else []
		self.delete = delete

	async def run(self, tree):
		if self.srcdir:
			src = os.path.join(self.srcroot, self.srcdir) + "/"
		else:
			src = os.path.normpath(self.srcroot) + "/"
		if self.destdir:
			dest = os.path.join(tree.root, self.destdir) + "/"
		else:
			if self.srcdir:
				dest = os.path.join(tree.root, self.srcdir) + "/"
			else:
				dest = os.path.normpath(tree.root) + "/"
		if not os.path.exists(dest):
			os.makedirs(dest)
		cmd = 'rsync -a --exclude CVS --exclude .svn --filter="hide /.git" --filter="protect /.git" '
		for e in self.exclude:
			cmd += "--exclude %s " % e
		if self.delete:
			cmd += "--delete --delete-excluded "
		cmd += "%s %s" % (src, dest)
		await runShell(cmd)


class GenerateRepoMetadata(MergeStep):
	def __init__(self, name, masters=None, aliases=None, priority=None):
		self.name = name
		self.aliases = aliases if aliases is not None else []
		self.masters = masters if masters is not None else []
		self.priority = priority

	async def run(self, tree):
		meta_path = os.path.join(tree.root, "metadata")
		if not os.path.exists(meta_path):
			os.makedirs(meta_path)
		a = open(meta_path + "/layout.conf", "w")
		out = (
			"""repo-name = %s
thin-manifests = true
sign-manifests = false
profile-formats = portage-2
cache-formats = md5-dict
"""
			% self.name
		)
		if self.aliases:
			out += "aliases = %s\n" % " ".join(self.aliases)
		if self.masters:
			out += "masters = %s\n" % " ".join(self.masters)
		a.write(out)
		a.close()
		rn_path = os.path.join(tree.root, "profiles")
		if not os.path.exists(rn_path):
			os.makedirs(rn_path)
		a = open(rn_path + "/repo_name", "w")
		a.write(self.name + "\n")
		a.close()


class RemoveFiles(MergeStep):
	def __init__(self, globs=None):
		if globs is None:
			globs = []
		self.globs = globs

	async def run(self, tree):
		for glob in self.globs:
			cmd = "rm -rf %s/%s" % (tree.root, glob)
			await runShell(cmd)


class CopyAndRename(MergeStep):
	def __init__(self, src, dest, ren_fun):
		self.src = src
		self.dest = dest
		# renaming function ... accepts source file path, and returns destination filename
		self.ren_fun = ren_fun

	async def run(self, tree):
		srcpath = os.path.join(tree.root, self.src)
		for f in os.listdir(srcpath):
			destfile = os.path.join(tree.root, self.dest)
			destfile = os.path.join(destfile, self.ren_fun(f))
			await runShell("( cp -a %s/%s %s )" % (srcpath, f, destfile))


class SyncFiles(MergeStep):
	def __init__(self, srcroot, files):
		self.srcroot = srcroot
		self.files = files
		if not isinstance(files, dict):
			raise TypeError("'files' argument should be a dict of source:destination items")

	async def run(self, tree):
		for src, dest in self.files.items():
			if dest is not None:
				dest = os.path.join(tree.root, dest)
			else:
				dest = os.path.join(tree.root, src)
			src = os.path.join(self.srcroot, src)
			if os.path.exists(dest):
				print("%s exists, attempting to unlink..." % dest)
				try:
					os.unlink(dest)
				except (IOError, PermissionError) as e:
					print("Unlinking failed: %s" % str(e))
					pass
			dest_dir = os.path.dirname(dest)
			if os.path.exists(dest_dir) and os.path.isfile(dest_dir):
				os.unlink(dest_dir)
			if not os.path.exists(dest_dir):
				os.makedirs(dest_dir)
			print("copying %s to final location %s" % (src, dest))
			shutil.copyfile(src, dest)


class CleanTree(MergeStep):
	# remove all files from tree, except dotfiles/dirs.

	def __init__(self, exclude=None):
		if exclude is None:
			exclude = []
		self.exclude = exclude

	async def run(self, tree):
		for fn in os.listdir(tree.root):
			if fn[:1] == ".":
				continue
			if fn in self.exclude:
				continue
			await runShell("rm -rf %s/%s" % (tree.root, fn))


class ELTSymlinkWorkaround(MergeStep):
	async def run(self, tree):
		dest = os.path.join(tree.root + "/eclass/ELT-patches")
		if not os.path.lexists(dest):
			os.makedirs(dest)


regextype = type(re.compile("hello, world"))


class InsertFilesFromSubdir(MergeStep):
	def __init__(self, srctree, subdir, suffixfilter=None, select="all", skip=None, src_offset=""):
		self.subdir = subdir
		self.suffixfilter = suffixfilter
		self.select = select
		self.srctree = srctree
		self.skip = skip
		self.src_offset = src_offset

	async def run(self, desttree):
		desttree.logTree(self.srctree)
		src = self.srctree.root
		if self.src_offset:
			src = os.path.join(src, self.src_offset)
		if self.subdir:
			src = os.path.join(src, self.subdir)
		if not os.path.exists(src):
			return
		dst = desttree.root
		if self.subdir:
			dst = os.path.join(dst, self.subdir)
		if not os.path.exists(dst):
			os.makedirs(dst)
		for e in os.listdir(src):
			if self.suffixfilter and not e.endswith(self.suffixfilter):
				continue
			if isinstance(self.select, list):
				if e not in self.select:
					continue
			elif isinstance(self.select, regextype):
				if not self.select.match(e):
					continue
			if isinstance(self.skip, list):
				if e in self.skip:
					continue
			elif isinstance(self.skip, regextype):
				if self.skip.match(e):
					continue
			real_dst = os.path.basename(os.path.join(dst, e))
			await runShell("cp -a %s/%s %s" % (src, e, dst))


class InsertEclasses(InsertFilesFromSubdir):
	def __init__(self, srctree, select="all", skip=None):
		InsertFilesFromSubdir.__init__(self, srctree, "eclass", ".eclass", select=select, skip=skip)


class InsertLicenses(InsertFilesFromSubdir):
	def __init__(self, srctree, select="all", skip=None):
		InsertFilesFromSubdir.__init__(self, srctree, "licenses", select=select, skip=skip)


class CreateCategories(MergeStep):
	async def run(self, desttree):
		catset = set()
		for maybe_cat in os.listdir(desttree.root):
			full_path = os.path.join(desttree.root, maybe_cat)
			if not os.path.isdir(full_path):
				continue
			if "-" in maybe_cat or maybe_cat == "virtual":
				catset.add(maybe_cat)
		if not os.path.exists(desttree.root + "/profiles"):
			os.makedirs(desttree.root + "/profiles")
		with open(desttree.root + "/profiles/categories", "w") as g:
			for cat in sorted(list(catset)):
				g.write(cat + "\n")


class ZapMatchingEbuilds(MergeStep):
	def __init__(self, srctree, select="all", branch=None):
		self.select = select
		self.srctree = srctree
		self.branch = branch

	async def run(self, desttree):
		if self.branch is not None:
			# Allow dynamic switching to different branches/commits to grab things we want:
			await self.srctree.gitCheckout(branch=self.branch)
		# Figure out what categories to process:
		dest_cat_path = os.path.join(desttree.root, "profiles/categories")
		if os.path.exists(dest_cat_path):
			with open(dest_cat_path, "r") as f:
				dest_cat_set = set(f.read().splitlines())
		else:
			dest_cat_set = set()

		# Our main loop:
		print("# Zapping builds from %s" % desttree.root)
		for cat in os.listdir(desttree.root):
			if cat not in dest_cat_set:
				continue
			src_catdir = os.path.join(self.srctree.root, cat)
			if not os.path.isdir(src_catdir):
				continue
			for src_pkg in os.listdir(src_catdir):
				dest_pkgdir = os.path.join(desttree.root, cat, src_pkg)
				if not os.path.exists(dest_pkgdir):
					# don't need to zap as it doesn't exist
					continue
				await runShell("rm -rf %s" % dest_pkgdir)


class RecordAllCatPkgs(MergeStep):
	"""
	This is used for non-auto-generated kits where we should record the catpkgs as belonging to a particular kit
	but perform no other action. A kit generation NO-OP, comparted to InsertEbuilds
	"""

	def __init__(self, hub, srctree: GitTree):
		self.srctree = srctree
		self.hub = hub

	async def run(self, desttree=None):
		for catpkg in self.srctree.getAllCatPkgs():
			self.hub.CPM_LOGGER.record(self.srctree.name, catpkg, is_fixup=False)


class InsertEbuilds(MergeStep):
	"""
	Insert ebuilds in source tre into destination tree.

	select: Ebuilds to copy over.
		By default, all ebuilds will be selected. This can be modified by setting select to a
		list of ebuilds to merge (specify by catpkg, as in "x11-apps/foo"). It is also possible
		to specify "x11-apps/*" to refer to all source ebuilds in a particular category.

	skip: Ebuilds to skip.
		By default, no ebuilds will be skipped. If you want to skip copying certain ebuilds,
		you can specify a list of ebuilds to skip. Skipping will remove additional ebuilds from
		the set of selected ebuilds. Specify ebuilds to skip using catpkg syntax, ie.
		"x11-apps/foo". It is also possible to specify "x11-apps/*" to skip all ebuilds in
		a particular category.

	replace: Ebuilds to replace.
		By default, if an catpkg dir already exists in the destination tree, it will not be overwritten.
		However, it is possible to change this behavior by setting replace to True, which means that
		all catpkgs should be overwritten. It is also possible to set replace to a list containing
		catpkgs that should be overwritten. Wildcards such as "x11-libs/*" will be respected as well.

	categories: Categories to process.
		categories to process for inserting ebuilds. Defaults to all categories in tree, using
		profiles/categories and all dirs with "-" in them and "virtuals" as sources.


	"""

	def __init__(
		self,
		hub,
		srctree: GitTree,
		select="all",
		select_only="all",
		skip=None,
		replace=False,
		categories=None,
		ebuildloc=None,
		move_maps: dict = None,
		skip_duplicates=True,
	):
		self.select = select
		self.skip = skip
		self.srctree = srctree
		self.replace = replace
		self.categories = categories
		self.hub = hub
		self.skip_duplicates = skip_duplicates
		if move_maps is None:
			self.move_maps = {}
		else:
			self.move_maps = move_maps
		if select_only is None:
			self.select_only = []
		else:
			self.select_only = select_only
		self.ebuildloc = ebuildloc

	def __repr__(self):
		return "<InsertEbuilds: %s>" % self.srctree.root

	async def run(self, desttree):

		if self.ebuildloc:
			srctree_root = self.srctree.root + "/" + self.ebuildloc
		else:
			srctree_root = self.srctree.root

		if self.srctree.should_autogen:
			await self.srctree.autogen(src_offset=self.ebuildloc)

		desttree.logTree(self.srctree)
		# Figure out what categories to process:
		src_cat_path = os.path.join(srctree_root, "profiles/categories")
		dest_cat_path = os.path.join(desttree.root, "profiles/categories")
		if self.categories is not None:
			# categories specified in __init__:
			src_cat_set = set(self.categories)
		else:
			src_cat_set = set()
			if os.path.exists(src_cat_path):
				# categories defined in profile:
				with open(src_cat_path, "r") as f:
					src_cat_set.update(f.read().splitlines())
			# auto-detect additional categories:
			cats = os.listdir(srctree_root)
			for cat in cats:
				# All categories have a "-" in them and are directories:
				if os.path.isdir(os.path.join(srctree_root, cat)):
					if "-" in cat or cat == "virtual":
						src_cat_set.add(cat)
		if os.path.exists(dest_cat_path):
			with open(dest_cat_path, "r") as f:
				dest_cat_set = set(f.read().splitlines())
		else:
			dest_cat_set = set()
		# Our main loop:
		print("# Merging in ebuilds from %s" % srctree_root)
		for cat in src_cat_set:
			catdir = os.path.join(srctree_root, cat)
			if not os.path.isdir(catdir):
				# not a valid category in source overlay, so skip it
				continue
			# runShell("install -d %s" % catdir)
			for pkg in os.listdir(catdir):
				catpkg = "%s/%s" % (cat, pkg)
				pkgdir = os.path.join(catdir, pkg)
				if self.select_only != "all" and catpkg not in self.select_only:
					# we don't want this catpkg
					continue
				if not os.path.isdir(pkgdir):
					# not a valid package dir in source overlay, so skip it
					continue
				if isinstance(self.select, list):
					if catpkg not in self.select:
						# we have a list of pkgs to merge, and this isn't on the list, so skip:
						continue
				elif isinstance(self.select, regextype):
					if not self.select.match(catpkg):
						# no regex match:
						continue
				if isinstance(self.skip, list):
					if catpkg in self.skip:
						# we have a list of pkgs to skip, and this catpkg is on the list, so skip:
						continue
				elif isinstance(self.skip, regextype):
					if self.select.match(catpkg):
						# regex skip match, continue
						continue
				dest_cat_set.add(cat)
				tpkgdir = None
				tcatpkg = None
				if catpkg in self.move_maps:
					if os.path.exists(pkgdir):
						# old package exists, so we'll want to rename.
						tcatpkg = self.move_maps[catpkg]
						tpkgdir = os.path.join(desttree.root, tcatpkg)
					else:
						tcatpkg = self.move_maps[catpkg]
						# old package doesn't exist, so we'll want to use the "new" pkgname as the source, hope it's there...
						pkgdir = os.path.join(srctree_root, tcatpkg)
						# and use new package name as destination...
						tpkgdir = os.path.join(desttree.root, tcatpkg)
				else:
					tpkgdir = os.path.join(desttree.root, catpkg)
				tcatdir = os.path.dirname(tpkgdir)
				copied = False
				if self.replace is True or (isinstance(self.replace, list) and (catpkg in self.replace)):
					if not os.path.exists(tcatdir):
						os.makedirs(tcatdir)
					if os.path.exists(tpkgdir):
						await runShell("rm -rf " + tpkgdir)
					await runShell(["/bin/cp", "-a", pkgdir, tpkgdir])
					copied = True
				else:
					if not os.path.exists(tpkgdir):
						copied = True
					if not os.path.exists(tcatdir):
						os.makedirs(tcatdir)
					if not os.path.exists(tpkgdir):
						await runShell(["/bin/cp", "-a", pkgdir, tpkgdir])
				if os.path.exists("%s/__pycache__" % tpkgdir):
					await runShell("rm -rf %s/__pycache__" % tpkgdir)
				if copied:
					# log XML here.
					if self.hub.CPM_LOGGER:
						self.hub.CPM_LOGGER.recordCopyToXML(self.srctree, desttree, catpkg)
						if isinstance(self.select, regextype):
							# If a regex was used to match the copied catpkg, record the regex.
							self.hub.CPM_LOGGER.record(desttree.name, catpkg, regex_matched=self.select)
						else:
							# otherwise, record the literal catpkg matched.
							self.hub.CPM_LOGGER.record(desttree.name, catpkg)
							if tcatpkg is not None:
								# This means we did a package move. Record the "new name" of the package, too. So both
								# old name and new name get marked as being part of this kit.
								self.hub.CPM_LOGGER.record(desttree.name, tcatpkg)
		if os.path.isdir(os.path.dirname(dest_cat_path)):
			with open(dest_cat_path, "w") as f:
				f.write("\n".join(sorted(dest_cat_set)))


class ProfileDepFix(MergeStep):
	"""ProfileDepFix undeprecates profiles marked as deprecated."""

	async def run(self, tree):
		fpath = os.path.join(tree.root, "profiles/profiles.desc")
		if os.path.exists(fpath):
			a = open(fpath, "r")
			for line in a:
				if line[0:1] == "#":
					continue
				sp = line.split()
				if len(sp) >= 2:
					prof_path = sp[1]
					await runShell("rm -f %s/profiles/%s/deprecated" % (tree.root, prof_path))


class RunSed(MergeStep):
	"""
	Run sed commands on specified files.

	files: List of files.

	commands: List of commands.
	"""

	def __init__(self, files, commands):
		self.files = files
		self.commands = commands

	async def run(self, tree):
		commands = list(itertools.chain.from_iterable(("-e", command) for command in self.commands))
		files = [os.path.join(tree.root, file) for file in self.files]
		await runShell(["sed"] + commands + ["-i"] + files)


class GenCache(MergeStep):
	"""GenCache runs egencache --update to update metadata."""

	def __init__(self, cache_dir=None, release=None):
		self.cache_dir = cache_dir
		self.release = release

	async def run(self, tree):

		if tree.name != "core-kit":
			repos_conf = (
				"[DEFAULT]\nmain-repo = core-kit\n\n[core-kit]\nlocation = %s/core-kit\n\n[%s]\nlocation = %s\n"
				% (tree.config.kit_dest, tree.reponame if tree.reponame else tree.name, tree.root)
			)

			# Perform QA check to ensure all eclasses are in place prior to performing egencache, as not having this can
			# cause egencache to hang.

			result = await getAllEclasses(tree, self.release)
			if None in result and len(result[None]):
				missing_eclasses = []
				for ec in result[None]:
					# if a missing eclass is not in core-kit, then we'll be concerned:
					if not os.path.exists("%s/core-kit/eclass/%s" % (tree.config.kit_dest, ec)):
						missing_eclasses.append(ec)
				if len(missing_eclasses):
					print("!!! Error: QA check on kit %s failed -- missing eclasses:" % tree.name)
					print("!!!      : " + " ".join(missing_eclasses))
					print(
						"!!!      : Please be sure to use kit-fixups or the overlay's eclass list to copy these necessary eclasses into place."
					)
					sys.exit(1)
		else:
			repos_conf = "[DEFAULT]\nmain-repo = core-kit\n\n[core-kit]\nlocation = %s/core-kit\n" % tree.config.kit_dest
		cmd = [
			"egencache",
			"--update",
			"--tolerant",
			"--repo",
			tree.reponame if tree.reponame else tree.name,
			"--repositories-configuration",
			repos_conf,
			"--config-root=/tmp",
			"--jobs",
			repr(multiprocessing.cpu_count() * 2),
		]
		if self.cache_dir:
			cmd += ["--cache-dir", self.cache_dir]
			if not os.path.exists(self.cache_dir):
				os.makedirs(self.cache_dir)
				os.chown(self.cache_dir, -1, grp.getgrnam("portage").gr_gid)
		attempts = 10
		attempt = 1
		while attempt <= attempts:
			if attempt != 1:
				print("Restarting egencache -- sometimes it dies... this is expected.")
			success = await runShell(cmd, abort_on_failure=False)
			if success:
				break
			attempt += 1
		if attempt > attempts:
			print("Couldn't get egencache to finish. Exiting.")
			sys.exit(1)


class GenUseLocalDesc(MergeStep):
	"""GenUseLocalDesc runs egencache to update use.local.desc"""

	async def run(self, tree):
		if tree.name != "core-kit":
			repos_conf = (
				"[DEFAULT]\nmain-repo = core-kit\n\n[core-kit]\nlocation = %s/core-kit\n\n[%s]\nlocation = %s\n"
				% (tree.config.kit_dest, tree.reponame if tree.reponame else tree.name, tree.root)
			)
		else:
			repos_conf = "[DEFAULT]\nmain-repo = core-kit\n\n[core-kit]\nlocation = %s/core-kit\n" % tree.config.kit_dest
		await runShell(
			[
				"egencache",
				"--update-use-local-desc",
				"--tolerant",
				"--config-root=/tmp",
				"--repo",
				tree.reponame if tree.reponame else tree.name,
				"--repositories-configuration",
				repos_conf,
			],
			abort_on_failure=False,
		)


class GitCheckout(MergeStep):
	def __init__(self, branch):
		self.branch = branch

	async def run(self, tree):
		await runShell(
			"(cd %s && git checkout %s || git checkout -b %s --track origin/%s || git checkout -b %s)"
			% (tree.root, self.branch, self.branch, self.branch, self.branch)
		)


class CreateBranch(MergeStep):
	def __init__(self, branch):
		self.branch = branch

	async def run(self, tree):
		await runShell("( cd %s && git checkout -b %s --track origin/%s )" % (tree.root, self.branch, self.branch))


class Minify(MergeStep):
	"""Minify removes ChangeLogs and shrinks Manifests."""

	async def run(self, tree):
		await runShell("( cd %s && find -iname ChangeLog | xargs rm -f )" % tree.root, abort_on_failure=False)
		await runShell("( cd %s && find -iname Manifest | xargs -i@ sed -ni '/^DIST/p' @ )" % tree.root)
