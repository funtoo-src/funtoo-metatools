#!/usr/bin/env python3

import json
import logging
import os
import sys
from collections import defaultdict
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

import dyne.org.funtoo.metatools.merge as merge


def copy_from_fixups_steps(ctx):

	# Phase 3: copy eclasses, licenses, profile info, and ebuild/eclass fixups from the kit-fixups repository.

	# First, we are going to process the kit-fixups repository and look for ebuilds and eclasses to replace. Eclasses can be
	# overridden by using the following paths inside kit-fixups:

	# kit-fixups/eclass/1.2-release <--------- global eclasses, get installed to all kits unconditionally for release (overrides those above)
	# kit-fixups/<kit>/global/eclass <-------- global eclasses for a particular kit, goes in all branches (overrides those above)
	# kit-fixups/<kit>/global/profiles <------ global profile info for a particular kit, goes in all branches (overrides those above)
	# kit-fixups/<kit>/<branch>/eclass <------ eclasses to install in just a specific branch of a specific kit (overrides those above)
	# kit-fixups/<kit>/<branch>/profiles <---- profile info to install in just a specific branch of a specific kit (overrides those above)

	# Note that profile repo_name and categories files are excluded from any copying.

	# Ebuilds can be installed to kits by putting them in the following location(s):

	# kit-fixups/<kit>/global/cat/pkg <------- install cat/pkg into all branches of a particular kit
	# kit-fixups/<kit>/<branch>/cat/pkg <----- install cat/pkg into a particular branch of a kit

	# Remember that at this point, we may be missing a lot of eclasses and licenses from Gentoo. We will then perform a final sweep
	# of all catpkgs in the dest_kit and auto-detect missing eclasses from Gentoo and copy them to our dest_kit. Remember that if you
	# need a custom eclass from a third-party overlay, you will need to specify it in the overlay's overlays["ov_name"]["eclasses"]
	# list. Or alternatively you can copy the eclasses you need to kit-fixups and maintain them there :)

	steps = []
	# Here is the core logic that copies all the fix-ups from kit-fixups (eclasses and ebuilds) into place:
	eclass_release_path = "eclass/%s" % merge.model.RELEASE
	if os.path.exists(os.path.join(merge.model.FIXUP_REPO.root, eclass_release_path)):
		steps += [merge.steps.SyncDir(merge.model.FIXUP_REPO.root, eclass_release_path, "eclass")]
	fixup_dirs = ["global", "curated", ctx.kit.branch]
	for fixup_dir in fixup_dirs:
		fixup_path = ctx.kit.name + "/" + fixup_dir
		if os.path.exists(merge.model.FIXUP_REPO.root + "/" + fixup_path):
			if os.path.exists(merge.model.FIXUP_REPO.root + "/" + fixup_path + "/eclass"):
				steps += [
					merge.steps.InsertFilesFromSubdir(
						merge.model.FIXUP_REPO, "eclass", ".eclass", select="all", skip=None, src_offset=fixup_path
					)
				]
			if os.path.exists(merge.model.FIXUP_REPO.root + "/" + fixup_path + "/licenses"):
				steps += [
					merge.steps.InsertFilesFromSubdir(
						merge.model.FIXUP_REPO, "licenses", None, select="all", skip=None, src_offset=fixup_path
					)
				]
			if os.path.exists(merge.model.FIXUP_REPO.root + "/" + fixup_path + "/profiles"):
				steps += [
					merge.steps.InsertFilesFromSubdir(
						merge.model.FIXUP_REPO, "profiles", None, select="all", skip=["repo_name", "categories"], src_offset=fixup_path
					)
				]
			# copy appropriate kit readme into place:
			readme_path = fixup_path + "/README.rst"
			if os.path.exists(merge.model.FIXUP_REPO.root + "/" + readme_path):
				steps += [merge.steps.SyncFiles(merge.model.FIXUP_REPO.root, {readme_path: "README.rst"})]

			# We now add a step to insert the fixups, and we want to record them as being copied so successive kits
			# don't get this particular catpkg. Assume we may not have all these catpkgs listed in our package-set
			# file...

			steps += [
				merge.steps.InsertEbuilds(merge.model.FIXUP_REPO, ebuildloc=fixup_path, select="all", skip=None, replace=True)
			]
	return steps


async def get_deepdive_kit_items(ctx):

	"""
	This function will read on-disk metadata for a particular kit, and process it, splitting it into individual
	records for performing a bulk insert into MongoDB, for example. It will return a big collection of dicts
	in a list, ready for insertion. As part of this scan, Manifest data will be read from disk and hashes will
	be added to each record.

	We use this after a kit has been generated. We then grab the JSON of the metadata cache and prep it for
	writing into MongoDB.
	"""

	repo_obj = await checkout_kit(ctx, pull=False)

	# load on-disk JSON metadata cache into memory:
	merge.metadata.fetch_kit(repo_obj)

	bulk_insert = []
	head_sha1 = merge.tree.headSHA1(repo_obj.root)
	# Grab our fancy JSON record containing lots of kit information and prep it for insertion into MongoDB:
	try:
		for atom, json_data in repo_obj.KIT_CACHE.items():
			json_data["commit"] = head_sha1
			sys.stdout.write(".")
			sys.stdout.flush()
			bulk_insert.append(json_data)
	except KeyError as ke:
		print(f"Encountered error when processing {ctx.kit.name} {ctx.kit.branch}")
		raise ke
	merge.metadata.flush_kit(repo_obj, save=False)
	print(f"Got {len(bulk_insert)} items to bulk insert for {ctx.kit.name} branch {ctx.kit.branch}.")
	return ctx, bulk_insert


async def checkout_kit(ctx, pull=None):

	# For Sabayon, if ctx.kit has a URL, we use that. This allows us to checkout anything as a "kit"
	# for leveraging our kit-cache pipeline.

	branch = ctx.kit.branch
	kwargs = {}

	# TODO: in the scenario where you might want to generate a 'dev' version of Funtoo, you will want
	#       to auto-create all auto-generated kits, but push them to a remote location. Likewise, you will
	#       want to auto-clone the 'Official' independent kits, but then rewrite their origin and push them
	#       to the same remote location. We should support this workflow and currently don't.

	# For auto-generated kits, if we are in 'dev mode' then simply create a Tree from scratch.
	kwargs["url"] = merge.model.MERGE_CONFIG.url(ctx.kit.name, kind="auto")
	kwargs["create_branches"] = merge.model.CREATE_BRANCHES
	# Allow overriding of pull behavior.
	if pull is not None:
		kwargs["pull"] = pull

	try:
		if merge.model.MIRROR:
			kwargs["mirror"] = merge.model.MERGE_CONFIG.mirror.rstrip("/") + "/" + ctx.kit.name
	except AttributeError:
		pass

	git_class = getattr(merge.model, "GIT_CLASS", merge.tree.GitTree)
	root = get_kit_root(ctx.kit.name)
	logging.warning(f"git class {ctx.kit.name} {branch}" )
	if not merge.model.PROD and merge.model.MERGE_CONFIG.destination_base_url:
		kwargs["fixup_origin"] = merge.model.MERGE_CONFIG.destination_base_url
	out_tree = git_class(ctx.kit.name, branch=branch, root=root, **kwargs)
	out_tree.initialize()

	# TODO: If an independent kit, and we are setting up a 'dev' branch, rewrite origin so we
	#       will push up to the 'dev' location.

	# TODO: If an auto-generated kit, we will want to still be able to push up to a remote location

	return out_tree


def get_kit_root(kit_name):
	if getattr(merge.model, "NEST_KITS", True):
		return os.path.join(merge.model.MERGE_CONFIG.dest_trees, "meta-repo/kits", kit_name)
	else:
		return os.path.join(merge.model.MERGE_CONFIG.dest_trees, kit_name)


def wipe_indy_kits():
	indy_roots = set()
	for kit_dict in merge.model.KIT_GROUPS:
		if kit_dict["kind"] == "independent":
			indy_roots.add(get_kit_root(kit_dict["name"]))
	for root in indy_roots:
		if os.path.exists(root):
			merge.tree.run_shell(f"rm -rf {root}")


async def generate_kit(ctx):

	"""

	This function will auto-generate a single 'autogenerated' kit by checking out the current version, wiping the
	contents of the git repo, and copying everything over again, updating metadata cache, etc. and then committing (and
	possibly pushing) the result.

	It will also work for 'independent' kits but will simply re-generate the metadata cache and ensure proper
	housekeeping is done.

	'ctx' is a NamespaceDict which contains the kit dictionary at `ctx.kit`.

	"""

	out_tree = await checkout_kit(ctx)

	# load on-disk JSON metadata cache into memory:
	merge.metadata.fetch_kit(out_tree)

	steps = []
	active_repos = set()

	if ctx.kit.kind == "independent":
		if ctx.kit.process is False:
			return ctx, out_tree, out_tree.head()
		steps += [merge.steps.RemoveFiles(["metadata/md5-cache"])]
	elif ctx.kit.kind == "sourced":
		steps += [merge.steps.CleanTree()]
		pre_steps, post_steps = merge.foundations.get_kit_pre_post_steps(ctx)
		from_tree = merge.model.SOURCE_REPOS[ctx.kit.source]
		steps += [
			merge.steps.JustAutogen(from_tree),
			merge.steps.SyncFromTree(from_tree)
		]
		steps += copy_from_fixups_steps(ctx)
		steps += [
			merge.steps.FindAndRemove(["__pycache__"]),
		] + post_steps
	elif ctx.kit.kind == "autogenerated":
		steps += [merge.steps.CleanTree()]

		pre_steps, post_steps = merge.foundations.get_kit_pre_post_steps(ctx)

		if pre_steps is not None:
			steps += pre_steps

		# Copy files specified in 'eclasses' and 'copyfiles' sections in the kit's YAML:
		for repo_name, copyfile_tuples in merge.foundations.get_copyfiles_from_yaml(ctx).items():
			steps += [merge.steps.CopyFiles(merge.model.SOURCE_REPOS[repo_name], copyfile_tuples)]

		# Copy over catpkgs listed in 'packages' section:

		for repo_name, packages in merge.foundations.get_kit_packages(ctx):
			active_repos.add(repo_name)
			from_tree = merge.model.SOURCE_REPOS[repo_name]
			# TODO: add move maps below
			steps += [merge.steps.InsertEbuilds(from_tree, skip=None, replace=True, move_maps=None, select=packages)]

		# If an autogenerated kit, we also want to copy various things (catpkgs, eclasses, profiles) from kit-fixups:
		steps += copy_from_fixups_steps(ctx)
		steps += [
			merge.steps.RemoveFiles(merge.foundations.get_excludes_from_yaml(ctx)),
			merge.steps.FindAndRemove(["__pycache__"]),
		] + post_steps

	steps += [
		merge.steps.FindAndRemove(["COPYRIGHT.txt"]),  # replaced with COPYRIGHT.rst and may be in indy kits
		merge.steps.GenerateLicensingFile(sorted(list(active_repos))),
		merge.steps.Minify(),
		merge.steps.ELTSymlinkWorkaround(),
		merge.steps.CreateCategories(),
		merge.steps.SyncDir(merge.model.SOURCE_REPOS["gentoo-staging"].root, "licenses"),
	]

	await out_tree.run(steps)

	# Now, if we are core-kit, get hashes of all the eclasses so that we can generate metadata cache and use
	# it as needed. core-kit gets processed by itself as the first item in the pipeline. So these vars will
	# be available for all successive metadata gen runs:

	if ctx.kit.name == "core-kit":
		merge.model.ECLASS_ROOT = out_tree.root
		merge.model.ECLASS_HASHES = merge.metadata.get_eclass_hashes(merge.model.ECLASS_ROOT)

	# We will execute all the steps that we have queued up to this point, which will result in out_tree.KIT_CACHE
	# being populated with all the metadata from the kit. Which will allow the next steps to run successfully.

	await out_tree.run([merge.steps.GenCache()])

	meta_steps = [merge.steps.PruneLicenses()]

	python_settings = merge.foundations.python_kit_settings()

	for py_branch, py_settings in python_settings.items():

		meta_steps += [merge.steps.GenPythonUse(py_settings, "funtoo/kits/python-kit/%s" % py_branch)]

	# We can now run all the steps that require access to metadata:

	await out_tree.run(meta_steps)

	if ctx.kit.kind == "independent":
		update_msg = "Automated updates by metatools for md5-cache and python profile settings."
	else:
		update_msg = "Autogenerated tree updates."

	out_tree.gitCommit(message=update_msg, push=merge.model.PUSH)

	# save in-memory metadata cache to JSON:
	merge.metadata.flush_kit(out_tree)

	return ctx, out_tree, out_tree.head()


def generate_metarepo_metadata(output_sha1s):
	"""
	Generates the metadata in /var/git/meta-repo/metadata/...
	:param release: the release string, like "1.3-release".
	:param merge.model.META_REPO: the meta-repo GitTree.
	:return: None.
	"""

	if not os.path.exists(merge.model.META_REPO.root + "/metadata"):
		os.makedirs(merge.model.META_REPO.root + "/metadata")

	with open(merge.model.META_REPO.root + "/metadata/kit-sha1.json", "w") as a:
		a.write(json.dumps(output_sha1s, sort_keys=True, indent=4, ensure_ascii=False))

	outf = merge.model.META_REPO.root + "/metadata/kit-info.json"
	all_kit_names = sorted(output_sha1s.keys())

	with open(outf, "w") as a:
		k_info = {}
		out_settings = defaultdict(lambda: defaultdict(dict))
		for kit_dict in merge.model.KIT_GROUPS:
			kit_name = kit_dict["name"]
			# specific keywords that can be set for each branch to identify its current quality level
			out_settings[kit_name]["stability"][kit_dict["branch"]] = kit_dict["stability"]
			kind = kit_dict["kind"]
			if kind != "independent":
				kind_json = "auto"
			elif kind == "independent":
				kind_json = "indy"
			else:
				raise ValueError(f"For kit {kit_name}: Kit type of {kind} not recognized.")
			out_settings[kit_name]["type"] = kind_json
		k_info["kit_order"] = all_kit_names
		k_info["kit_settings"] = out_settings

		# auto-generate release-defs. We used to define them manually in foundation:
		rdefs = {}
		for kit_name in all_kit_names:
			rdefs[kit_name] = []
			for def_kit in filter(
				lambda x: x["name"] == kit_name and x["stability"] not in ["deprecated"], merge.model.KIT_GROUPS
			):
				rdefs[kit_name].append(def_kit["branch"])

		rel_info = merge.foundations.release_info()

		k_info["release_defs"] = rdefs
		k_info["release_info"] = rel_info
		a.write(json.dumps(k_info, sort_keys=True, indent=4, ensure_ascii=False))

	with open(merge.model.META_REPO.root + "/metadata/version.json", "w") as a:
		a.write(json.dumps(rel_info, sort_keys=True, indent=4, ensure_ascii=False))


def mirror_repository(repo_obj, base_path):
	"""
	Mirror a repository to its mirror location, ie. GitHub.
	"""

	os.makedirs(base_path, exist_ok=True)
	merge.tree.run_shell(f"git clone --bare {repo_obj.root} {base_path}/{repo_obj.name}.pushme")
	merge.tree.run_shell(
		f"cd {base_path}/{repo_obj.name}.pushme && git remote add upstream {repo_obj.mirror} && git push --mirror upstream"
	)
	merge.tree.run_shell(f"rm -rf {base_path}/{repo_obj.name}.pushme")
	return repo_obj.name


def mirror_all_repositories():
	base_path = os.path.join(merge.model.MERGE_CONFIG.temp_path, "mirror_repos")
	merge.tree.run_shell(f"rm -rf {base_path}")
	kit_mirror_futures = []
	with ThreadPoolExecutor(max_workers=8) as executor:
		# Push all kits, then push meta-repo.
		for kit_name, kit_tuple in merge.model.KIT_RESULTS.items():
			ctx, tree_obj, tree_sha1 = kit_tuple
			future = executor.submit(mirror_repository, tree_obj, base_path)
			kit_mirror_futures.append(future)
		for future in as_completed(kit_mirror_futures):
			kit_name = future.result()
			print(f"Mirroring of {kit_name} complete.")
	merge.kit.mirror_repository(merge.model.META_REPO, base_path)
	print("Mirroring of meta-repo complete.")
