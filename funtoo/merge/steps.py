#!/usr/bin/python3
import itertools
import os
import re
import shutil

import dyne.org.funtoo.metatools.merge as merge

# TODO: add checks for duplicate catpkgs
# TODO: add checks for missing catpkgs
import jinja2


class MergeStep:

	# This is only used for Repository Steps:
	collector = None

	async def run(self, tree):
		pass


class GenerateLicensingFile(MergeStep):
	def __init__(self, active_repo_names):
		self.active_repo_names = active_repo_names

	async def run(self, kit):
		with open(os.path.join(merge.model.FIXUP_REPO.root, "COPYRIGHT.rst.tmpl"), "r") as lic_temp:
			template = jinja2.Template(lic_temp.read())
		with open(os.path.join(kit.root, "COPYRIGHT.rst"), "wb") as lic_out:
			lic_out.write(
				template.render(kit=kit, copyright=merge.foundations.get_copyright_rst(self.active_repo_names)).encode("utf-8")
			)


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
				b.write("gentoo\t" + mirrors + " " + " ".join(ls[1:]) + "\n")
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
		merge.tree.run_shell(cmd)


class SyncFromTree(SyncDir):
	# sync a full portage tree, deleting any excess files in the target dir:
	def __init__(self, srctree, exclude=None):
		if exclude is None:
			exclude = []
		self.srctree = srctree
		SyncDir.__init__(self, srctree.root, srcdir=None, destdir=None, exclude=exclude, delete=True)

	async def run(self, desttree):
		await SyncDir.run(self, desttree)
		desttree.logTree(self.srctree)


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


class RemoveIfExists(MergeStep):
	def __init__(self, files):
		self.files = files

	async def run(self, tree):
		for file in self.files:
			path = os.path.join(tree.root, file)
			if os.path.exists(file):
				os.unlink(path)


class FindAndRemove(MergeStep):
	def __init__(self, globs=None):
		if globs is None:
			globs = []
		self.globs = globs

	async def run(self, tree):
		for glob in self.globs:
			cmd = f"find {tree.root} -name {glob} -exec rm -rf {{}} +"
			merge.tree.run_shell(cmd, abort_on_failure=False)


class RemoveFiles(MergeStep):
	def __init__(self, globs=None):
		if globs is None:
			globs = []
		self.globs = globs

	async def run(self, tree):
		for glob in self.globs:
			cmd = "rm -rf %s/%s" % (tree.root, glob)
			merge.tree.run_shell(cmd)


class CopyFiles(MergeStep):
	"""
	Copy regular files from source tree `srctree` to destination.

	`file_map_tuples` has the format::

	  [ ( 'path/to/src', 'path/to/dest' ), ... ]

	Source and destination paths are relative paths, relative to `srctree` and `desttree` respectively.

	An assumption is made that we are copying regular files, so that we can properly create source directories
	if they do not exist.

	"""

	def __init__(self, srctree, file_map_tuples):
		self.srctree = srctree
		self.file_map_tuples = file_map_tuples

	async def run(self, desttree):
		for src_path, dst_path in self.file_map_tuples:
			f_src_path = os.path.join(self.srctree.root, src_path)
			if not os.path.exists(f_src_path):
				raise FileNotFoundError(f"Source file not found: {f_src_path}.")
			f_dst_path = os.path.join(desttree.root, dst_path)
			if os.path.exists(f_dst_path):
				os.unlink(f_dst_path)
			parent = os.path.dirname(f_dst_path)
			if not os.path.exists(parent):
				os.makedirs(parent, exist_ok=True)
			merge.tree.run_shell(f"cp -a {f_src_path} {f_dst_path}")


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
			merge.tree.run_shell(f"cp -a {srcpath}/{f} {destfile}")


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
		files = ""
		for fn in os.listdir(tree.root):
			if fn[:1] == ".":
				continue
			if fn in self.exclude:
				continue
			files += " '" + fn + "'"
		merge.tree.run_shell(f"cd {tree.root} && rm -rf {files[1:]}")


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
			merge.tree.run_shell("cp -a %s/%s %s" % (src, e, dst))


class InsertEclasses(InsertFilesFromSubdir):
	def __init__(self, srctree, select="all", skip=None):
		InsertFilesFromSubdir.__init__(self, srctree, "eclass", ".eclass", select=select, skip=skip)


class PruneLicenses(MergeStep):

	"""

	This step will remove all files in licenses/ that is not actually used by any ebuild in the kit. This
	step expects KIT_CACHE to be populated (so GenCache() should be run first.)

	"""

	def get_all_licenses(self, desttree):
		used_licenses = set()
		for key, datums in desttree.KIT_CACHE.items():
			metadata = datums["metadata"]
			if metadata and "LICENSE" in metadata:
				used_licenses = used_licenses | set(metadata["LICENSE"].split())
		return used_licenses

	async def run(self, desttree):
		if os.path.exists(desttree.root + "/licenses"):
			used_licenses = self.get_all_licenses(desttree)
			to_remove = []
			for license in os.listdir(desttree.root + "/licenses"):
				if license not in used_licenses:
					to_remove.append(desttree.root + "/licenses/" + license)
			for file in to_remove:
				os.unlink(file)


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
			self.srctree.gitCheckout(branch=self.branch)
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
				merge.tree.run_shell("rm -rf %s" % dest_pkgdir)


class JustAutogen(MergeStep):

	def __init__( self, srctree):
		self.srctree = srctree

	async def run(self, desttree):

		await self.srctree.autogen()


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
		srctree,
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

		script_out = ""
		checks = []

		if self.ebuildloc:
			srctree_root = self.srctree.root + "/" + self.ebuildloc
		else:
			srctree_root = self.srctree.root

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
						script_out += f"/bin/rm -rf '{tpkgdir}' \n"
					script_out += f"/bin/cp -a '{pkgdir}' '{tpkgdir}'\n"
					checks.append(tpkgdir)
					copied = True
				else:
					if not os.path.exists(tpkgdir):
						copied = True
					if not os.path.exists(tcatdir):
						os.makedirs(tcatdir)
					if not os.path.exists(tpkgdir):
						script_out += f"/bin/cp -a '{pkgdir}' '{tpkgdir}'\n"
						checks.append(tpkgdir)
				if copied:
					# log XML here.
					pass
		if script_out:
			temp_out = os.path.join(merge.model.MERGE_CONFIG.temp_path, desttree.name + "_copyfiles.sh")
			os.makedirs(os.path.dirname(temp_out), exist_ok=True)
			with open(temp_out, "w") as f:
				f.write("#!/bin/bash\n")
				f.write(script_out)
			merge.tree.run_shell(f"/bin/bash {temp_out}")
			os.unlink(temp_out)
		for check in checks:
			if not os.path.exists(check):
				raise FileNotFoundError(
					f"It appears that {check} was not copied successfully. Maybe missing from {self.srctree.name}?"
				)


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
					merge.tree.run_shell("rm -f %s/profiles/%s/deprecated" % (tree.root, prof_path))


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
		merge.tree.run_shell(["sed"] + commands + ["-i"] + files)


class GenCache(MergeStep):
	"""GenCache runs egencache --update to update metadata."""

	def __init__(self, cache_dir=None, release=None):
		self.cache_dir = cache_dir
		self.release = release

	async def run(self, tree):
		merge.metadata.gen_cache(tree)


class Minify(MergeStep):
	"""Minify removes ChangeLogs and shrinks Manifests."""

	async def run(self, tree):
		merge.tree.run_shell("( cd %s && find -iname ChangeLog | xargs rm -f )" % tree.root, abort_on_failure=False)
		merge.tree.run_shell("( cd %s && find -iname Manifest | xargs -i@ sed -ni '/^DIST/p' @ )" % tree.root)


class GenPythonUse(MergeStep):
	def __init__(self, py_settings, out_subpath):
		self.def_python = py_settings["primary"]
		self.bk_python = py_settings["alternate"]
		self.mask = py_settings["mask"]
		self.out_subpath = out_subpath

	async def run(self, cur_overlay):
		all_lines = []
		for catpkg, cpv_list in merge.metadata.get_catpkg_from_cpvs(cur_overlay.KIT_CACHE.keys()).items():
			result = await merge.metadata.get_python_use_lines(
				cur_overlay, catpkg, cpv_list, cur_overlay.root, self.def_python, self.bk_python
			)
			if result is not None:
				all_lines += result

		all_lines = sorted(all_lines)
		outpath = cur_overlay.root + "/profiles/" + self.out_subpath + "/package.use"
		if not os.path.exists(outpath):
			os.makedirs(outpath)
		with open(outpath + "/python-use", "w") as f:
			for l in all_lines:
				f.write(l + "\n")
		# for core-kit, set good defaults as well.
		if cur_overlay.name == "core-kit":
			outpath = cur_overlay.root + "/profiles/" + self.out_subpath + "/make.defaults"
			a = open(outpath, "w")
			a.write('PYTHON_TARGETS="%s %s"\n' % (self.def_python, self.bk_python))
			a.write('PYTHON_SINGLE_TARGET="%s"\n' % self.def_python)
			a.close()
			if self.mask:
				outpath = cur_overlay.root + "/profiles/" + self.out_subpath + "/package.mask/funtoo-kit-python"
				if not os.path.exists(os.path.dirname(outpath)):
					os.makedirs(os.path.dirname(outpath))
				a = open(outpath, "w")
				a.write(self.mask + "\n")
				a.close()
