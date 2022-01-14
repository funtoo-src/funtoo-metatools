#!/usr/bin/env python3

import glob
import json
import logging
import os
import re
import subprocess
from collections import defaultdict

import dyne.org.funtoo.metatools.merge as merge
from dict_tools.data import NamespaceDict

#################################################################################################################
# This file contains the more 'grizzly' low-level parts of the kit-cache generation code. It is used by
# funtoo/merge/kit.py.
#################################################################################################################

from metatools.tree import run

#################################################################################################################
# Increment this constant whenever we update the kit-cache to store new data. If what we retrieve is an earlier
# version, we'll consider the kit cache stale and regenerate it.
#################################################################################################################

CACHE_DATA_VERSION = "1.0.6"

METADATA_LINES = [
	"DEPEND",
	"RDEPEND",
	"SLOT",
	"SRC_URI",
	"RESTRICT",
	"HOMEPAGE",
	"LICENSE",
	"DESCRIPTION",
	"KEYWORDS",
	"INHERITED",
	"IUSE",
	"REQUIRED_USE",
	"PDEPEND",
	"BDEPEND",
	"EAPI",
	"PROPERTIES",
	"DEFINED_PHASES",
	"HDEPEND",
	"PYTHON_COMPAT",
]

AUXDB_LINES = sorted(
	[
		"DEPEND",
		"RDEPEND",
		"SLOT",
		"SRC_URI",
		"RESTRICT",
		"HOMEPAGE",
		"LICENSE",
		"DESCRIPTION",
		"KEYWORDS",
		"IUSE",
		"REQUIRED_USE",
		"PDEPEND",
		"BDEPEND",
		"EAPI",
		"PROPERTIES",
		"DEFINED_PHASES",
	]
)


def cleanup_error_logs():
	# This should be explicitly called at the beginning of every command that generates metadata for kits:

	for file in glob.glob(os.path.join(merge.model.temp_path, "metadata-errors*.log")):
		os.unlink(file)


def display_error_summary():
	for stat_list, name, shortname in [
		(merge.model.metadata_error_stats, "metadata extraction errors", "errors"),
		(merge.model.processing_warning_stats, "warnings", "warnings"),
	]:
		if len(stat_list):
			for stat_info in stat_list:
				stat_info = NamespaceDict(stat_info)
				logging.warning(f"The following kits had {name}:")
				branch_info = f"{stat_info.name} branch {stat_info.branch}".ljust(30)
				logging.warning(f"* {branch_info} -- {stat_info.count} {shortname}.")
			logging.warning(f"{name} errors logged to {merge.model.temp_path}.")


def strip_rev(s):
	"""
	A short function to strip the revision from the end of an ebuild, returning either
	`( 'string_with_revision_missing', '<revision_num_as_string>' )` or
	`( 'original_string', None )` if no revision was found.
	"""

	num_strip = s.rstrip("0123456789")
	if num_strip != s and num_strip[-2:] == "-r":
		rev_strip = num_strip[:-2]
		rev = s[len(num_strip) :]
		return rev_strip, rev
	return s, None


def get_catpkg_from_cpvs(cpv_list):
	"""
	This function takes a list of things that look like 'sys-apps/foboar-1.2.0-r1' and returns a dict of
	unique catpkgs found (as dict keys) and exact matches (in dict value, as a member of a set.)

	Note that the input to this function must have version information. This method is not designed to
	distinguish between non-versioned atoms and versioned ones.
	"""
	catpkgs = defaultdict(set)
	for cpv in cpv_list:
		reduced, rev = strip_rev(cpv)
		last_hyphen = reduced.rfind("-")
		cp = cpv[:last_hyphen]
		catpkgs[cp].add(cpv)
	return catpkgs


def get_eapi_of_ebuild(ebuild_path):
	"""
	This function is used to parse the first few lines of the ebuild looking for an EAPI=
	line. This is annoying but necessary.
	"""

	# This pattern is specified by PMS section 7.3.1.
	_pms_eapi_re = re.compile(r"^[ \t]*EAPI=(['\"]?)([A-Za-z0-9+_.-]*)\1[ \t]*([ \t]#.*)?$")
	_comment_or_blank_line = re.compile(r"^\s*(#.*)?$")

	def _parse_eapi_ebuild_head(f):
		eapi = None
		eapi_lineno = None
		lineno = 0
		for line in f:
			lineno += 1
			m = _comment_or_blank_line.match(line)
			if m is None:
				eapi_lineno = lineno
				m = _pms_eapi_re.match(line)
				if m is not None:
					eapi = m.group(2)
				break

		return (eapi, eapi_lineno)

	with open(ebuild_path, "r") as fobj:
		return _parse_eapi_ebuild_head(fobj.readlines())


def extract_manifest_hashes(man_file):
	"""
	Given a manifest path as an argument, attempt to open `Manifest` and extract all digests for each
	DIST entry, and return this info along with filesize in a dict.
	"""
	man_info = {}
	if os.path.exists(man_file):
		with open(man_file, "r") as man_f:
			for line in man_f.readlines():
				ls = line.split()
				if len(ls) <= 3 or ls[0] != "DIST":
					continue
				pos = 3
				digests = {}
				while pos < len(ls):
					hash_type = ls[pos].lower()
					hash_digest = ls[pos + 1]
					digests[hash_type] = hash_digest
					pos += 2
				man_info[ls[1]] = {"size": ls[2], "hashes": digests}
	return man_info


def extract_uris(src_uri):
	"""
	This function will take a SRC_URI value from an ebuild, and it will return a dictionary in the following format:

	{ "filename1.tar.gz" : { "src_uri" : [ "https://url1", "https//url2" ] } }

	All possible download locations will be returned for files in the format above.

	Note that in the code below, a "blob" is simply a piece of parsed SRC_URI information that *may* be a URL.
	"""
	fn_urls = {}

	def record_fn_url(my_fn, p_blob):
		if my_fn not in fn_urls:
			fn_urls[my_fn] = {"src_uri": [p_blob]}
		else:
			fn_urls[my_fn]["src_uri"].append(p_blob)

	blobs = src_uri.split()
	prev_blob = None
	pos = 0

	while pos <= len(blobs):
		if pos < len(blobs):
			blob = blobs[pos]
		else:
			blob = ""
		if blob in [")", "(", "||"] or blob.endswith("?"):
			pos += 1
			continue
		if blob == "->":
			# We found a http://foo -> bar situation. Handle it:
			try:
				fn = blobs[pos + 1]
			except IndexError:
				# A -> at the end of a SRC_URI. Shouldn't happen but you never know.
				fn = None
			if fn is not None:
				record_fn_url(fn, prev_blob)
				prev_blob = None
			pos += 2
		else:
			# Process previous item:
			if prev_blob:
				fn = prev_blob.split("/")[-1]
				record_fn_url(fn, prev_blob)
			prev_blob = blob
			pos += 1

	return fn_urls


def get_catpkg_relations_from_depstring(depstring):
	"""
	This is a handy function that will take a dependency string, like something you would see in DEPEND, and it will
	return a set of all catpkgs referenced in the dependency string. It does not evaluate USE conditionals, nor does
	it return any blockers.

	This method is used to determine package relationships, in a general sense. Does one package reference another
	package in a dependency in some way? That's what this is used for.

	What is returned is a python set of catpkg atoms (no version info, just cat/pkg).
	"""
	catpkgs = set()

	for part in depstring.split():

		# 1. Strip out things we are not interested in:
		if part in ["(", ")", "||"]:
			continue
		if part.endswith("?"):
			continue
		if part.startswith("!"):
			# we are not interested in blockers
			continue

		# 2. For remaining catpkgs, strip comparison operators:
		has_version = False
		for op in [">=", "<=", ">", "<", "=", "~"]:
			if part.startswith(op):
				part = part[len(op) :]
				has_version = True
				break

		# 3. From the end, strip SLOT and USE info:
		for ender in [":", "["]:
			# strip everything from slot or USE spec onwards
			pos = part.rfind(ender)
			if pos == -1:
				continue
			part = part[:pos]

		# 4. Strip any trailing '*':
		part = part.rstrip("*")

		# 5. We should now have a catpkg or catpgkg-version(-rev). If we have this, remove it.
		if has_version:
			ps = part.split("-")
			has_rev = False
			if ps[-1].startswith("r"):
				try:
					int(ps[-1][1:])
					has_rev = True
				except ValueError:
					pass
			if has_rev:
				strip = 2
			else:
				strip = 1
			part = "-".join(ps[:-strip])

		catpkgs.add(part)
	return catpkgs


def extract_ebuild_metadata(kit_gen_obj, atom, ebuild_path=None, env=None, eclass_paths=None):
	"""
	TODO: This function is hard-coded to assume a python3.7 installation. Should be relatively easy to
	      tweak this to auto-detect PORTAGE_BIN_PATH.
	"""
	infos = {"HASH_KEY": atom}
	env["PATH"] = "/bin:/usr/bin"
	env["LC_COLLATE"] = "POSIX"
	env["LANG"] = "en_US.UTF-8"
	# For things to work correctly, the EAPI of the ebuild has to be manually extracted:
	eapi, lineno = get_eapi_of_ebuild(ebuild_path)
	if eapi is not None and eapi in "01234567":
		env["EAPI"] = eapi
	else:
		env["EAPI"] = "0"
	env["PORTAGE_GID"] = "250"
	env["PORTAGE_BIN_PATH"] = "/usr/lib/portage/python3.7"
	#env["PORTAGE_ECLASS_LOCATIONS"] = " ".join(eclass_paths)
	env["EBUILD"] = ebuild_path
	env["EBUILD_PHASE"] = "depend"
	# TODO: turn off:
	env["ECLASS_DEBUG_OUTPUT"] = "on"
	# This tells ebuild.sh to write out the metadata to stdout (fd 1) which is where we will grab
	# it from:
	env["PORTAGE_PIPE_FD"] = "1"
	cmdstr = "export PORTAGE_ECLASS_LOCATIONS=(\n"
	for eclass_path in eclass_paths:
		cmdstr += f"  {eclass_path}\n"
	cmdstr += ")\n"
	#echo LOCATIONS IS $PORTAGE_ECLASS_LOCATIONS[@]\n"
	ebuild_sh_path = os.path.join(env["PORTAGE_BIN_PATH"], "ebuild.sh")
	cmdstr += f". {ebuild_sh_path}\n"
	merge.model.log.info(cmdstr)
	#result = run("/bin/bash -c \"" + cmdstr + "\"", env=env)

	success = False
	err_out = ""
	with subprocess.Popen(["/bin/bash", "-c", cmdstr], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
		if proc.returncode == 0:
			success = True
		else:
			output, err_out = proc.communicate()
	if not success:
		err_out = err_out.decode("utf-8")
		kit_gen_obj.metadata_errors[atom] = {"status": "ebuild.sh failure", "output": err_out}
		merge.model.log.error(f"ebuild.sh failure: {err_out}")
		return None
	try:
		lines = output.decode("utf-8").split("\n")
		line = 0
		found = set()
		while line < len(METADATA_LINES) and line < len(lines):
			found.add(METADATA_LINES[line])
			infos[METADATA_LINES[line]] = lines[line]
			line += 1
		if line != len(METADATA_LINES):
			missing = set(METADATA_LINES) - found
			kit_gen_obj.metadata_errors[atom] = {"status": "missing " + " ".join(missing), "output": err_out}
			return None
		# Success! Clear previous error, if any:
		if atom in kit_gen_obj.metadata_errors:
			del kit_gen_obj.metadata_errors[atom]
		return infos
	except (FileNotFoundError, IndexError) as e:
		kit_gen_obj.metadata_errors[atom] = {"status": "exception", "exception": str(e)}
		return None


def get_filedata(src_uri, manifest_path):
	"""
	This function is given `src_uri` which is the literal `SRC_URI` data from an ebuild, and a path to a `Manifest`
	for the catpkg.

	What is returned is a list of dictionaries. Each dictionary represents a file that will be downloaded for a
	particular ebuild.

	Each dictionary has the following keys:

	*. `name` (dest. filename),
	*. `src_uri` (a list of URIs to download the file, and may include 'mirror://' URLs),
	*. `size` (size of file in bytes)
	*. `hashes` (digests from the `Manifest` file associated with this file.

	Note that any files that appear in the `Manifest` but not in `SRC_URI` are ignored. This function is purely
	intended to "complete" the `SRC_URI` data with data that is in the `Manifest`.

	This function uses two sub-functions to do most of the dirty work, and then merges the results.

	MongoDB is happiest when we don't use filenames as keys, since they have periods in them which is not allowed.
	This normalizes our filedata for MongoDB. `extract_uris` and `extract_manifest_hashes` are all indexed by filename,
	but instead we want to return a list consisting of dictionaries. We move the key inside each dict.

	{"file1.tar.gz" : { ... }} -> [ { "name" : "file1.tar.gz", ... }, ... ]
	"""

	filedata = extract_manifest_hashes(manifest_path)
	extracted_uris = extract_uris(src_uri)

	for fn, sub_dict in extracted_uris.items():
		# just augment SRC_URI data with Manifest data, if available.
		if fn in filedata:
			extracted_uris[fn].update(filedata[fn])

	outdata = []
	for fn, datums in extracted_uris.items():
		datums["name"] = fn
		outdata.append(datums)

	return outdata


def catpkg_generator(repo_path=None):
	"""
	This function is a generator that will scan a specified path for all valid category/
	package directories (catpkgs). It will yield paths to these directories. It defines
	a valid catpkg as a path two levels deep that contains at least one .ebuild file.
	"""

	cpdirs = defaultdict(set)

	for catdir in os.listdir(repo_path):
		catpath = os.path.join(repo_path, catdir)
		if not os.path.isdir(catpath):
			continue
		for pkgdir in os.listdir(catpath):
			pkgpath = os.path.join(catpath, pkgdir)
			if not os.path.isdir(pkgpath):
				continue
			for ebfile in os.listdir(pkgpath):
				if ebfile.endswith(".ebuild"):
					if pkgdir not in cpdirs[catdir]:
						cpdirs[catdir].add(pkgdir)
						yield os.path.join(pkgpath)

# TODO: maybe change this name to post_actions(). And integrate Manifest generation here. We want
#       to avoiding having MANIFEST_LINES or integrate MANIFEST_LINES better into the kit-cache.
#       This is not ABSOLUTELY necessary but may make things a bit simpler. MANIFEST_LINES was
#       created before we had the kit-cache and deepdive.


async def get_python_use_lines(kit_gen, catpkg, cpv_list, cur_tree, def_python, bk_python):
	# TODO: This should be fixed or replaced, because there's hard-coded thangs in here.
	ebs = {}
	for cpv in cpv_list:
		metadata = kit_gen.kit_cache[cpv]["metadata"]
		if not metadata:
			imps = []
		else:
			imps = metadata["PYTHON_COMPAT"].split()

		# For anything in PYTHON_COMPAT that we would consider equivalent to python3_7, we want to
		# set python3_7 instead. This is so we match the primary python implementation correctly
		# so we don't incorrectly enable the backup python implementation. We basically have to
		# mirror the exact mapping logic in python-utils-r1.eclass.

		new_imps = set()
		for imp in imps:
			if imp in ["python3_5", "python3_6"]:
				# The eclass bumps these to python3_7. We do the same to get correct results:
				new_imps.add(def_python)
			elif imp in ["python3+", "python3_7+"]:
				new_imps.update(["python3_7", "python3_8", "python3_9"])
			elif imp == "python3.8+":
				new_imps.update(["python3_8", "python3_9"])
			elif imp == "python3.9+":
				new_imps.add("python3_9")
			elif imp == "python2+":
				new_imps.update(["python2_7", "python3_7", "python3_8", "python3_9"])
			else:
				new_imps.add(imp)
		imps = list(new_imps)
		if len(imps):
			ebs[cpv] = imps

	# ebs now is a dict containing catpkg -> PYTHON_COMPAT settings for each ebuild in the catpkg. We want to see if they are identical
	# if split == False, then we will do one global setting for the catpkg. If split == True, we will do individual settings for each version
	# of the catpkg, since there are differences. This saves space in our python-use file while keeping everything correct.

	oldval = None
	split = False
	for key, val in ebs.items():
		if oldval is None:
			oldval = val
		else:
			if oldval != val:
				split = True
				break
	lines = []
	if len(ebs.keys()):
		if not split:
			line = do_package_use_line(catpkg, def_python, bk_python, oldval)
			if line is not None:
				lines.append(line)
		else:
			for key, val in ebs.items():
				line = do_package_use_line("=%s" % key, def_python, bk_python, val)
				if line is not None:
					lines.append(line)
	return lines


def do_package_use_line(pkg, def_python, bk_python, imps):
	out = None
	if def_python not in imps:
		if bk_python in imps:
			out = "%s python_single_target_%s" % (pkg, bk_python)
		else:
			out = "%s python_single_target_%s python_targets_%s" % (pkg, imps[0], imps[0])
	return out


def load_json(fn, validate=True):
	"""
	This is a stand-alone function for loading kit cache JSON data, in case someone like me wants to manually load
	it and look at it. It will check to make sure the CACHE_DATA_VERSION matches what this code is designed to
	inspect, by default.
	"""
	with open(fn, "r") as f:
		kit_cache_data = json.loads(f.read())
		if validate:
			if "cache_data_version" not in kit_cache_data:
				logging.error("JSON invalid or missing cache_data_version.")
				return None
			elif kit_cache_data["cache_data_version"] != CACHE_DATA_VERSION:
				logging.error(f"Cache data version is {kit_cache_data['cache_data_version']} but needing {CACHE_DATA_VERSION}")
				return None
		return kit_cache_data


def get_atom(kit_gen_obj, atom, md5, manifest_md5):
	"""
	Read from our in-memory kit metadata cache. Return something if available, else None.

	This will validate that our in-memory record has a matching md5 and that md5s of all
	eclasses match. AND the md5 of the Manifest (if any exists) matches.
	Otherwise we treat this as a cache miss.
	"""
	existing = None
	if atom in kit_gen_obj.kit_cache and kit_gen_obj.kit_cache[atom]["md5"] == md5:
		existing = kit_gen_obj.kit_cache[atom]
		bad = False
		if "manifest_md5" not in existing:
			bad = True
		elif manifest_md5 != existing["manifest_md5"]:
			bad = True
		elif existing["eclasses"]:
			for eclass, md5 in existing["eclasses"]:
				if eclass not in kit_gen_obj.eclasses.hashes:
					bad = True
					break
				if kit_gen_obj.eclasses.hashes[eclass] != md5:
					bad = True
					break
		if bad:
			# stale cache entry, don't use.
			existing = None
	return existing

# vim: ts=4 sw=4 noet
