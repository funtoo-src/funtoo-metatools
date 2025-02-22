#!/usr/bin/env python3

import logging
from collections import defaultdict
from re import sub

import packaging
from packaging.version import Version

LICENSE_CLASSIFIER_MAP = {
	"License :: OSI Approved :: Apache Software License": "Apache-2.0",
	"License :: OSI Approved :: BSD License": "BSD",
	"License :: OSI Approved :: MIT License": "MIT",
}

log = logging.getLogger('metatools.autogen')


def pypi_license_to_gentoo(classifiers):
	"""
	This function will use our (currently very minimal) mapping of pypi license classifiers to Gentoo
	license names. Note that "||" syntax is not used since the classifiers do not support this.

	Empty string is returned if no license info is found, or if no licenses match.
	"""
	global LICENSE_CLASSIFIER_MAP
	license_set = set()
	for classifier in classifiers:
		if not classifier.startswith("License :: "):
			continue
		if classifier in LICENSE_CLASSIFIER_MAP:
			license_set.add(LICENSE_CLASSIFIER_MAP[classifier])
	return " ".join(sorted(list(license_set)))


def pypi_metadata_init(local_pkginfo, json_dict):
	"""
	This function initializes metadata for the package based on pypi (and also sets defaults for things like
	inherit.)
	"""
	if "inherit" not in local_pkginfo:
		local_pkginfo["inherit"] = []
	if "distutils-r1" not in local_pkginfo["inherit"]:
		local_pkginfo["inherit"].append("distutils-r1")
	if "desc" not in local_pkginfo and "summary" in json_dict["info"] and json_dict["info"]["summary"]:
		local_pkginfo["desc"] = sub(r"[^a-zA-Z0-9\s.]", "",json_dict["info"]["summary"])
	if "homepage" not in local_pkginfo and "home_page" in json_dict["info"]:
		local_pkginfo["homepage"] = f"{json_dict['info']['home_page']}"
		if "project_url" in json_dict["info"]:
			local_pkginfo["homepage"] += f" {json_dict['info']['project_url']}"
	if "license" not in local_pkginfo and "classifiers" in json_dict["info"]:
		local_pkginfo["license"] = pypi_license_to_gentoo(json_dict["info"]["classifiers"])


def get_sdist_package(release):
	for package in release:
		if package["packagetype"] == "sdist":
			return package
	return None


def pypi_normalize_name(pkginfo):
	if "pypi_name" not in pkginfo:
		pkginfo["pypi_name"] = pkginfo["name"]
	return pkginfo["pypi_name"]


def pypi_normalize_version(pkginfo):
	version_parts = pkginfo["version"].split(".")
	if version_parts[-1].startswith("post"):
		ebuild_version = ".".join(version_parts[:-1]) + "_p" + version_parts[-1][4:]
	else:
		ebuild_version = pkginfo["version"]
	pkginfo["pypi_version"] = pkginfo["version"]
	pkginfo["version"] = ebuild_version


def python_version_ok_lt(cur, req):
	cur_parts = cur.split(".")
	req_parts = req.split(".")
	while len(cur_parts) < 3:
		cur_parts.append("0")
	while len(req_parts) < 3:
		req_parts.append("0")
	cur_parts = list(map(int, cur_parts))
	req_parts = list(map(int, req_parts))
	for part in range(0, 3):
		cur = cur_parts[part]
		req = req_parts[part]
		if req > cur:
			return True
		elif req < cur:
			return False
	return False


def python_version_ok_ge(cur, req):
	cur_parts = cur.split(".")
	req_parts = req.split(".")
	while len(cur_parts) < 3:
		cur_parts.append("0")
	while len(req_parts) < 3:
		req_parts.append("0")
	cur_parts = list(map(int, cur_parts))
	req_parts = list(map(int, req_parts))
	for part in range(0,3):
		cur = cur_parts[part]
		req = req_parts[part]
		if req > cur:
			return False
		elif cur > req:
			return True
	return True


def python_version_ok_gt(cur, req):
	cur_parts = cur.split(".")
	req_parts = req.split(".")
	while len(cur_parts) < 3:
		cur_parts.append("0")
	while len(req_parts) < 3:
		req_parts.append("0")
	cur_parts = list(map(int, cur_parts))
	req_parts = list(map(int, req_parts))
	for part in range(0,3):
		cur = cur_parts[part]
		req = req_parts[part]
		if cur > req:
			return True
		elif cur < req:
			return False
	return False


def python_version_ok_ne(cur, req):
	cur_parts = cur.split(".")
	req_parts = req.split(".")
	while len(cur_parts) < 3:
		cur_parts.append("0")
	while len(req_parts) < 3:
		req_parts.append("0")
	for part in range(0,3):
		cur = cur_parts[part]
		req = req_parts[part]
		if req == "*":
			return False
		elif req != cur:
			return True
	return True


def python_version_ok(cur, release, requires_python_override=None):
	if requires_python_override:
		requires_python = requires_python_override
	else:
		if "requires_python" not in release or not release["requires_python"]:
			log.debug("No requires_python specified for release.")
			return True
		requires_python = release["requires_python"]
	# cur == "current" == the version we are checking.
	# req == "required" == the version from the JSON from pypi we're validating against
	result = False
	for req in requires_python.replace(" ", "").split(","):
		if not req:  # trailing comma will cause this
			continue
		if req.startswith(">="):
			result = python_version_ok_ge(cur, req[2:])
			log.debug(f"python_version_ok_ge {req} {result}")
		elif req.startswith(">"):
			result = python_version_ok_ge(cur, req[1:])
			log.debug(f"python_version_ok_gt {req} {result}")
		elif req.startswith("!="):
			result = python_version_ok_ne(cur, req[2:])
			log.debug(f"python_version_ok_ne {req} {result}")
		elif req.startswith("<") and req[1] != "=":
			result = python_version_ok_lt(cur, req[1:])
			log.debug(f"python_version_ok_lt {req} {result}")
		else:
			raise ValueError(f"WUT???? '{req}'")
	return result


def pypi_get_artifact_url(pkginfo, json_dict, strict=False, has_python=None, requires_python_override=None):
	"""
	Look in JSON data ``json_dict`` retrieved from pypi for the proper sdist artifact for the package specified in
	pkginfo. If ``strict`` is True, will insist on the ``version`` defined in ``pkginfo``, otherwise, will be flexible
	and fall back to most recent sdist.
	"""
	if strict:
		version = pkginfo["version"]
		if version not in json_dict["releases"]:
			raise ValueError(f"Package version {version} specified in YAML is not found.")
		sdist_package = get_sdist_package(json_dict["releases"][version])
		if not sdist_package:
			raise ValueError(f"Could not find an 'sdist' release for {pkginfo['name']} version {version} specified in YAML.")
		if has_python and not python_version_ok(has_python, sdist_package, requires_python_override=requires_python_override):
			raise ValueError(f"Specified package version {version} is not compatible with Python {has_python}. Please check.")
		return sdist_package["url"]
	else:
		sdist_package = None
		release_versions = []
		for v_str in json_dict["releases"].keys():
			try:
				v = Version(v_str)
				if not (v.is_devrelease or v.is_prerelease):
					release_versions.append(v)
			except packaging.version.InvalidVersion:
				log.warning(f"Invalid version on pypi, skipped: {v_str}")
		release_versions = list(sorted(release_versions, reverse=True))
		log.debug(f"pypi_get_artifact_url: considering these versions, in order: {release_versions}")
		for version_obj in release_versions:
			version = str(version_obj)
			log.debug(f"pypi_get_artifact_url: {version}")
			sdist_package = get_sdist_package(json_dict["releases"][version])
			if not sdist_package:
				continue
			if has_python and not python_version_ok(has_python, sdist_package, requires_python_override=requires_python_override):
				continue
			pkginfo["version"] = version
			break
		if sdist_package is None:
			raise ValueError(f"Was unable to find a version of {pkginfo['name']} compatible with Python: {has_python}")
		return sdist_package["url"]


def expand_pydep(pkginfo, pyatom):
	"""
	Takes something from our pydeps YAML that might be "foo", or "sys-apps/foo", or "foo >= 1.2" and convert to
	the proper Gentoo atom format.
	"""
	# TODO: support ranges?
	# TODO: pass a ctx variable here so we can have useful error messages about what pkg is triggering the error.
	psp = pyatom.split()
	if not len(psp):
		raise ValueError(f"{pkginfo['cat']}/{pkginfo['name']} appears to have invalid pydeps. Make sure each pydep is specified as a YAML list item starting with '-'.")
	if psp[0] == "not!":
		block = "!"
		psp = psp[1:]
	else:
		block = ""
	if len(psp) == 3 and psp[1] in [">", ">=", "<", "<="]:
		if "/" in psp[0]:
			# already has a category
			return f"{block}{psp[1]}{psp[0]}-{psp[2]}[${{PYTHON_USEDEP}}]"
		else:
			# inject dev-python
			return f"{block}{psp[1]}dev-python/{psp[0]}-{psp[2]}[${{PYTHON_USEDEP}}]"
	elif len(psp) == 1:
		if "/" in pyatom:
			return f"{block}{psp[0]}[${{PYTHON_USEDEP}}]"
		else:
			# inject dev-python
			return f"{block}dev-python/{psp[0]}[${{PYTHON_USEDEP}}]"
	else:
		raise ValueError(f"{pkginfo['cat']}/{pkginfo['name']} appears to have an invalid pydep '{pyatom}'.")


def create_ebuild_cond_dep(pkginfo, pydeplabel, atoms):
	"""
	This function takes a specifier like "py:all" and a list of simplified pythony package atoms and creates a
	conditional dependency for inclusion in an ebuild. It returns a list of lines (without newline termination,
	each string in the list implies a separate line.)
	"""
	out_atoms = []
	pyspec = None
	usespec = None
	if pydeplabel.dep_type == "py":
		pyspec = pydeplabel.gen_cond_dep()
	elif pydeplabel.dep_type == "use":
		usespec = list(pydeplabel.specifiers)[0]

	for atom in atoms:
		out_atoms.append(expand_pydep(pkginfo, atom))

	if usespec:
		out = [f"{usespec}? ( {' '.join(sorted(out_atoms))} )"]
	elif not len(pyspec):
		# no condition -- these deps are for all python versions, so not a conditional dep:
		out = out_atoms
	else:
		# stuff everything into a python_gen_cond_dep:
		out = [r"$(python_gen_cond_dep '" + ' '.join(sorted(out_atoms)) + r"' " + " ".join(sorted(pyspec)) + ")"]
	return out


class InvalidPyDepLabel(Exception):

	def __init__(self, label, errmsg=None):
		self.label = label
		self.errmsg = errmsg

	def __str__(self):
		out = f"{self.label.pydep_label}"
		if self.errmsg:
			out += " " + self.errmsg
		return out


class ParsedPyDepLabel:

	def __init__(self, pydep_label):
		self.pydep_label = pydep_label
		self.dep_type = None
		self.mods = set()
		self._ver_set = set()
		self.has_2x_version = False
		self.has_3x_version = False
		self.parse()

	def parse(self):
		parts = self.pydep_label.split(":")
		if not len(parts):
			raise InvalidPyDepLabel(self)
		if parts[0] not in ["use", "py"]:
			raise InvalidPyDepLabel(self)
		self.dep_type = parts[0]
		if len(parts) == 3:
			self.mods = set(parts[-1].split(","))
		if self.dep_type == "py":
			self._ver_set = set(parts[1].split(","))
		else:
			self._ver_set = {parts[1]}
		self._validate_ver_set()

	def _validate_ver_set(self):
		if self.dep_type != "py":
			return True
		if self._ver_set & {"3", "all"}:
			self.has_3x_version = True
		if self._ver_set & {"2", "all"}:
			self.has_2x_version = True
		remaining = self._ver_set - {"3", "2", "all", "pypy", "pypy3"}
		for ver_spec in list(remaining):
			if ver_spec.startswith("2."):
				self.has_2x_version = True
			elif ver_spec.startswith("3."):
				self.has_3x_version = True
			if ver_spec == "pypy3":
				self.has_3x_version = True
			remaining.remove(ver_spec)
		if len(remaining):
			raise InvalidPyDepLabel(self)

	@property
	def specifiers(self):
		return sorted(list(self._ver_set))

	def has_specifier(self, ver):
		return ver in self._ver_set

	@property
	def build_dep(self):
		return "build" in self.mods

	@property
	def post_dep(self):
		return "post" in self.mods

	@property
	def runtime_dep(self):
		return "runtime" in self.mods or len(self.mods) == 0

	@property
	def tool_dep(self):
		return "tool" in self.mods

	@property
	def py2_enabled(self):
		"""
		Tell us if this dependency should be enabled on compat ebuilds.
		"""
		if self.dep_type == "py" and not self.has_2x_version:
			return False
		return True

	@property
	def py3_enabled(self):
		"""
		Tell us if this dependency should be enabled on py3-only ebuilds.
		"""
		if self.dep_type == "py" and not self.has_3x_version:
			return False
		return True

	def gen_cond_dep(self):
		"""
		This method takes a parsed pydep label and converts it to a list of arguments that should
		be passed to python_gen_cond_dep (eclass function.) Protect ourselves from the weird syntax in this eclass.

		 py:all -> [] (meaning "no restriction", i.e. apply to all versions)
		 py:2,3.7,3.8 -> [ "-2", "python3_7", "python3_8"]

		"""
		assert self.dep_type == "py"
		if "all" in self._ver_set:
			return []
		out = []
		for pg_item in self._ver_set:
			if pg_item in ["2", "3"]:
				out += [f"-{pg_item}"]  # -2, etc.
			elif "." in pg_item:
				# 2.7 -> python2_7, etc.
				out += [f"python{pg_item.replace('.', '_')}"]
			else:
				# pass thru pypy, pypy3, etc.
				out.append(pg_item)
		return out


def expand_pydeps(pkginfo, compat_mode=False, compat_ebuild=False):
	expanded_pydeps = defaultdict(list)
	if "pydeps" in pkginfo:
		pytype = type(pkginfo["pydeps"])
		if pytype == list:
			for dep in pkginfo["pydeps"]:
				# super-simple pydeps are just considered runtime deps
				expanded_pydeps["rdepend"].append(expand_pydep(pkginfo, dep))
		elif pytype == dict:
			for label_str, deps in pkginfo["pydeps"].items():
				label = ParsedPyDepLabel(label_str)
				if compat_mode:
					if compat_ebuild and not label.py2_enabled:
						continue
					elif not compat_ebuild and not label.py3_enabled:
						continue
				cond_dep = create_ebuild_cond_dep(pkginfo, label, deps)
				if label.build_dep:
					expanded_pydeps["depend"] += cond_dep
				if label.runtime_dep:
					expanded_pydeps["rdepend"] += cond_dep
				if label.post_dep:
					expanded_pydeps["pdepend"] += cond_dep
				if label.tool_dep:
					expanded_pydeps["bdepend"] += cond_dep
	for dep_type in ["depend", "rdepend", "pdepend", "bdepend"]:
		deps = expanded_pydeps[dep_type]
		if not deps:
			continue
		if dep_type not in pkginfo:
			pkginfo[dep_type] = "\n".join(deps)
		else:
			pkginfo[dep_type] += "\n" + "\n".join(deps)
	return None


assert python_version_ok_lt("3.7.6", "4.0.0") is True
assert python_version_ok_lt("4.0.1", "4.0.0") is False
assert python_version_ok_lt("3.7.7", "3.7.7") is False
assert python_version_ok_lt("3.7.8", "2.7.18") is False
assert python_version_ok_gt("3.7.8", "3.7.9") is False
assert python_version_ok_gt("3.9.8", "3.7.9") is True
assert python_version_ok_gt("3.9.8", "3.9.8") is False
