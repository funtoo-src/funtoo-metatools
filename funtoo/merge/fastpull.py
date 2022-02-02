#!/usr/bin/python3

import logging
import os

import dyne.org.funtoo.metatools.pkgtools as pkgtools
import dyne.org.funtoo.metatools.merge as merge


def get_third_party_mirrors():
	if not merge.model.THIRD_PARTY_MIRRORS:
		merge.model.THIRD_PARTY_MIRRORS = merge.metadata.get_thirdpartymirrors(
			os.path.expanduser("~/repo_tmp/dest-trees/meta-repo/kits/core-kit")
		)
	return merge.model.THIRD_PARTY_MIRRORS


def expand_uris(src_uri_list):
	real_uri = []
	for src_uri in src_uri_list:
		if src_uri.startswith("mirror://"):
			real_uri.append(merge.metadata.expand_thirdpartymirror(get_third_party_mirrors(), src_uri))
		else:
			slash_split = src_uri.split("/")
			if len(slash_split) == 0:
				continue
			elif slash_split[0] not in ["http:", "https:", "ftp:"]:
				continue
			real_uri.append(src_uri)
	return real_uri


def complete_artifact(artifact):
	"""
	Provided with an artifact and expected final data (hashes and size), we will attempt to locate the artifact
	binary data in the fastpull database. If we find it, we 'complete' the artifact so it is usable for extraction
	or looking at final hashes, with a correct on-disk path to where the data is located.

	Note that when we look for the completed artifact, we don't care if our data has a different 'name' -- as long
	as the binary data on disk has matching hashes and size.

	If not found, simply return None.

	This method was originally intended to allow us to specify expected final data, aka hashes, that we expect to
	see. But this is not really used by autogen at the moment. The reason is that while emerge and ebuild do
	Manifest/hash validation on the client side, this is because we want to ensure that what was downloaded by the
	client matches what was set by the server. But we don't have such checks on just the server side.
	"""
	fp = artifact.fastpull_path
	if not fp:
		return None
	hashes = pkgtools.download.calc_hashes(fp)
	if hashes["sha512"] != artifact.final_data["sha512"]:
		return None
	if hashes["size"] != artifact.final_data["size"]:
		return None
	artifact.final_data = hashes
	artifact.final_path = fp
	return artifact


def get_disk_path(sh):
	return os.path.join(merge.model.MERGE_CONFIG.fastpull_path, sh[:2], sh[2:4], sh[4:6], sh)


async def inject_into_fastpull(artifact):
	"""
	For a given artifact, make sure it's fetched locally and then add it to the fastpull archive.
	"""
	success = await artifact.ensure_fetched()
	if not success:
		return
	fastpull_path = artifact.fastpull_path
	if os.path.islink(fastpull_path):
		# This will fix-up the situation where we used symlinks in fastpull rather than copying the file. It will
		# replace the symlink with the actual file. I did this for quickly migrating the legacy fastpull db. Once
		# I have migrated it over, this condition can probably be safely removed.
		actual_file = os.path.realpath(fastpull_path)
		if os.path.exists(actual_file):
			os.unlink(fastpull_path)
			os.link(actual_file, fastpull_path)
	elif not os.path.exists(fastpull_path):
		try:
			os.makedirs(os.path.dirname(fastpull_path), exist_ok=True)
			os.link(artifact.final_path, fastpull_path)
			logging.warning(f"Object {artifact.final_path} injected into fastpull.")
		except Exception as e:
			# Multiple doits running in parallel, trying to link the same file -- could cause exceptions:
			logging.error(f"Exception encountered when trying to link into fastpull (may be harmless) -- {repr(e)}")


def parse_mcafee_logs(logf, path_prefix="/opt"):
	"""
	This method takes a McAfee Virus Scanner log file as an argument, and will scan the log for filenames that showed
	up in it. This is useful for when things in the fastpull mirror are showing up in a McAfee scan. The function will
	then extract the sha512's and return these as a list.

	The McAfee logs use "-" at the end of line to indicate the line is continued on the next line. This function has
	to potentially re-assemble split sha512 digests, as well as detect mutiple files listed on a single line.
	"""
	out_digests = []
	with open(logf, "r") as f:
		lines = f.readlines()
		new_lines = []
		pos = 0
		while pos < len(lines):
			cur_line = lines[pos].strip()
			pos += 1
			if path_prefix not in cur_line:
				continue

			while cur_line.endswith("-"):
				cur_line = cur_line[:-1] + lines[pos + 1]
				pos += 1

			new_lines.append(cur_line)

		for line in new_lines:
			ls = line.split()
			for part in ls:
				if part.startswith("/opt"):
					part_strip = part.rstrip(",")
					out_digests.append(part_strip.split("/")[-1])

	return out_digests
