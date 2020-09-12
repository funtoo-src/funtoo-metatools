#!/usr/bin/env python3

import yaml
import os

from merge.steps import GenerateRepoMetadata, SyncDir, ThirdPartyMirrors, RunSed, SyncFiles


def __init__(hub):
	with open(os.path.join(hub.FIXUP_REPO.root, "foundations.yaml"), "r") as f:
		hub.FDATA = yaml.safe_load(f)


def get_kit_pre_post_steps(hub, kit_dict):
	kit_steps = {
		"core-kit": {
			"pre": [
				GenerateRepoMetadata("core-kit", aliases=["gentoo"], priority=1000),
				# core-kit has special logic for eclasses -- we want all of them, so that third-party overlays can reference the full set.
				# All other kits use alternate logic (not in kit_steps) to only grab the eclasses they actually use.
				SyncDir(hub.SOURCE_REPOS["gentoo-staging"].root, "eclass"),
			],
			"post": [
				# news items are not included here anymore
				SyncDir(hub.FIXUP_REPO.root, "metadata", exclude=["cache", "md5-cache", "layout.conf"]),
				# add funtoo stuff to thirdpartymirrors
				ThirdPartyMirrors(),
				RunSed(["profiles/base/make.defaults"], ["/^PYTHON_TARGETS=/d", "/^PYTHON_SINGLE_TARGET=/d"]),
			],
		},
		# masters of core-kit for regular kits and nokit ensure that masking settings set in core-kit for catpkgs in other kits are applied
		# to the other kits. Without this, mask settings in core-kit apply to core-kit only.
		"regular-kits": {"pre": [GenerateRepoMetadata(kit_dict["name"], masters=["core-kit"], priority=500),]},
		"all-kits": {
			"pre": [SyncFiles(hub.FIXUP_REPO.root, {"COPYRIGHT.txt": "COPYRIGHT.txt", "LICENSE.txt": "LICENSE.txt",}),]
		},
		"nokit": {"pre": [GenerateRepoMetadata("nokit", masters=["core-kit"], priority=-2000),]},
	}

	out_pre_steps = []
	out_post_steps = []

	kd = kit_dict["name"]
	if kd in kit_steps:
		if "pre" in kit_steps[kd]:
			out_pre_steps += kit_steps[kd]["pre"]
		if "post" in kit_steps[kd]:
			out_post_steps += kit_steps[kd]["post"]

	# a 'regular kit' is not core-kit or nokit -- if we have pre or post steps for them, append these steps:
	if kit_dict["name"] not in ["core-kit", "nokit"] and "regular-kits" in kit_steps:
		if "pre" in kit_steps["regular-kits"]:
			out_pre_steps += kit_steps["regular-kits"]["pre"]
		if "post" in kit_steps["regular-kits"]:
			out_post_steps += kit_steps["regular-kits"]["post"]

	if "all-kits" in kit_steps:
		if "pre" in kit_steps["all-kits"]:
			out_pre_steps += kit_steps["all-kits"]["pre"]
		if "post" in kit_steps["all-kits"]:
			out_post_steps += kit_steps["all-kits"]["post"]

	return out_pre_steps, out_post_steps


def get_kit_items(hub, kit_name, section="packages"):
	fn = f"{hub.FIXUP_REPO.root}/{kit_name}/packages.yaml"
	with open(fn, "r") as f:
		pdata = yaml.safe_load(f)
		for package_set in pdata[section]:
			repo_name = list(package_set.keys())[0]
			packages = package_set[repo_name]
			yield repo_name, packages


def get_kit_packages(hub, kit_name):
	return hub._.get_kit_items(kit_name)


def release_exists(hub, release):
	for release_dict in hub.FDATA["kit-groups"]["releases"]:
		cur_release = list(release_dict.keys())[0]
		if cur_release == release:
			return True
	return False


def kit_groups(hub):
	defaults = hub.FDATA["kit-groups"]["defaults"] if "defaults" in hub.FDATA["kit-groups"] else {}
	for release_dict in hub.FDATA["kit-groups"]["releases"]:

		# unbundle from singleton dict:
		release = list(release_dict.keys())[0]
		release_data = release_dict[release]

		if release != hub.RELEASE:
			continue

		for kg in release_data:
			out = defaults.copy()
			if isinstance(kg, str):
				out["name"] = kg
			elif isinstance(kg, dict):
				out["name"] = list(kg.keys())[0]
				out.update(list(kg.values())[0])
			yield out
		break


def source_defs(hub, name):
	for sdef in hub.FDATA["source-defs"]:
		sdef_name = list(sdef.keys())[0]
		if sdef_name != name:
			continue
		sdef_data = list(sdef.values())[0]
		for sdef_entry in sdef_data:
			yield sdef_entry


def get_overlay(hub, name):
	"""
	Gets data on a specific overlay
	"""
	for ov_dict in hub.FDATA["overlays"]:

		if isinstance(ov_dict, str):
			ov_name = ov_dict
			ov_data = {"name": ov_name}
		else:
			ov_name = list(ov_dict.keys())[0]
			if ov_name != name:
				continue
			ov_data = list(ov_dict.values())[0]
			ov_data["name"] = ov_name

		if ov_name != name:
			continue

		url = hub.MERGE_CONFIG.get_option("sources", ov_name, None)
		if url is not None:
			ov_data["url"] = url

		if "url" not in ov_data:
			raise IndexError(f"No url found for overlay {name}")

		return ov_data
	raise IndexError(f"overlay not found: {name}")


def get_repos(hub, source_name):
	"""
	Given a source definition, return a list of repositories with all data included (like urls
	from the source definitions, etc.)
	"""

	sdefs = source_defs(hub, source_name)

	for repo_dict in sdefs:
		ov_name = repo_dict["repo"]
		ov_data = get_overlay(hub, ov_name)
		repo_dict.update(ov_data)

		if "src_sha1" not in repo_dict:
			branch = hub.MERGE_CONFIG.get_option("branches", ov_name, None)
			if branch is not None:
				repo_dict["branch"] = branch
			else:
				repo_dict["branch"] = "master"
		yield repo_dict