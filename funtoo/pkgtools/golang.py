import glob
import hashlib
import os
import shutil
import subprocess
import asyncio

from subpop.util import AttrDict


def escape_module_str(version):
	"""
	This method will escape a module string to comply with the Go Modules reference.

	https://golang.org/ref/mod#module-cache
	"""

	def escape_character(ch):
		if ch.isupper():
			return f"!{ch.lower()}"

		return ch

	return ''.join([escape_character(c) for c in version])


async def create_gosum_archive(hub, pkginfo):
	"""
	This is a helper function which interfaces with metatools' dynamic archives functionality and grabs
	a reference to an existing go module bundle if it exists locally, and if it doesn't, it gets it
	created. It does this by ensuring that all go modules are downloaded using our spider and then creates
	an archive and copies all the go modules inside.

	Note: autogen writers typically don't need to call this. Use ``add_gosum_archive`` instead.

	:param hub: your hub (local variable)
	:param pkginfo: your pkginfo
	:return: an ``Archive``, suitable to be added to ``SRC_URI``.
	"""
	gosum_bundle = pkginfo["gosum_bundle"]
	my_archive = hub.Archive(gosum_bundle.final_name)
	my_archive.initialize(f"funtoo-go-bundle-{pkginfo['name']}")
	module_artifacts = []
	for mod_attrs in gosum_bundle.mod_attrs_list:
		module_artifacts.append(hub.pkgtools.ebuild.Artifact(**mod_attrs))

	# Fetch dependencies in parallel
	await asyncio.gather(*[artifact.fetch() for artifact in module_artifacts])

	for artifact in module_artifacts:
		shutil.copy(artifact.blos_object.blob.path, os.path.join(my_archive.top_path, artifact.final_name))

	await my_archive.store(key=gosum_bundle["key"])
	return my_archive


def gen_gosum(gosum_path=None, gosum_data=None):
	"""
	This function generates gosum data for EGO_SUM variable used in ebuilds, and also returns a list of
	attributes to use to create new Artifacts for all go modules that needs to be downloaded based on the
	gosum.
	:param gosum_path: If provided, open this go.sum file and read its contents (string)
	:param gosum_data: If provided, this is a string containing the go.sum.
	:return: a tuple containing a string to use in EGO_SUM, plus a list of attributes to use to create
	         Artifacts.
	"""
	if not gosum_data:
		with open(gosum_path, "r") as f:
			gosum_lines = f.readlines()
	else:
		gosum_lines = gosum_data.split("\n")
	gosum = ""
	mod_attrs_list = []
	for line in gosum_lines:
		module = escape_module_str(line).split()
		if not len(module):
			continue
		gosum = gosum + '\t"' + module[0] + " " + module[1] + '"\n'
		module_path = module[0]
		module_ver = module[1].split("/")
		module_ext = "zip"
		if "go.mod" in module[1]:
			module_ext = "mod"
		module_uri = module_path + "/@v/" + module_ver[0] + "." + module_ext
		module_file = module_uri.replace("/", "%2F")
		mod_attrs_list.append(dict(url=f"https://proxy.golang.org/{module_uri}", final_name=module_file))
	return gosum, mod_attrs_list


async def get_gosum_artifacts(gosum_path):
	"""
	IMPORTANT: It's now preferred to use ``add_gosum_bundle`` instead, which uses dynamic archives and avoids
	having hundreds of files in SRC_URI.

	This method will extract package data from ``go.sum`` and generate Artifacts for all packages it finds.
	You must add ``${EGO_SUM_SRC_URI}`` to your ebuild which will result in a lot of downloads which happen
	one at a time.

	Note: There is an undefined reference to "hub." in this module. This is left because it doesn't actually
	break anything in autogens and fixing this would require an API change.
	"""
	gosum_artifacts = []
	gosum, mod_attrs_list = gen_gosum(gosum_path=gosum_path)
	for mod_attrs in mod_attrs_list:
		gosum_artifacts.append(
			hub.pkgtools.ebuild.Artifact(**mod_attrs)
		)
	return dict(gosum=gosum, gosum_artifacts=gosum_artifacts)


async def add_gosum_bundle(hub, pkginfo, gosum_data=None, gosum_path=None, src_artifact=None, src_dir_glob="*") -> None:
	"""
	BETA: API may change slightly before official release.

	This is the new, preferred way to support go modules in Funtoo.

	This method generates a bundle containing all necessary go modules, and then this archive
	can be used by your ebuild and Funtoo's updated go-module.eclass transparently.

	This solves the problem of having to download hundreds of individual go modules when you emerge
	something -- instead, you just download the bundle from our CDN, in one HTTP request rather than
	zillions of them.

	To use this function, you will want to call it from your autogen. There are a variety of ways to
	call it, due the variety of ways to grab a "go.sum".

	If you want to simply have this method grab the "go.sum" from an Artifact, the easiest way to do
	this is to set ``pkginfo['artifacts'] = { 'main' : <your artifact> }``. The Artifact doesn't need
	to be downloaded yet. The autogen will then look inside it for a ``go.sum``.

	Alternatively, you can use the ``src_artifact=`` keyword argument, if your source Artifact isn't
	in pkginfo.

	You can also use ``gosum_path`` to provide a path to a file which contains the gosum. This can be
	useful if you downloaded the gosum separately.

	Finally, you can use ``gosum_data`` to pass a raw string containing the entire gosum, if you
	happen to read it yourself.

	Upon completion, metatools will create its own archive from scratch and store it in the local
	binary object store, and copy it to $DISTDIR for you for convenience. You can reference the associated
	``Archive`` object via ``pkginfo["artifacts"]``, which will be created if it doesn't exist. If
	``pkginfo["artifacts"]`` is a dict, your bundle will be reachable at
	``pkginfo["artifacts"]["go_bundle"]``. Otherwise it will be tacked on the end of the artifacts list.

	To use this in a template, simply include all your artifacts in ``SRC_URI`` but leave out the
	traditional ``${EGO_SUM_SRC_URI}`` which will remove hundreds of emerge downloads from your ebuild::

	  SRC_URI="{{artifacts['main'].src_uri}}
      {{artifacts['go_bundle'].src_uri}}"

	The ``go-module.eclass`` in Funtoo will use this bundle automatically if it's included in ``SRC_URI``,
	and will identify it by its special name. It will then source all go modules from it rather than
	having to individually download go modules.
	"""

	pkginfo["gosum_bundle"] = AttrDict()

	# For convenience/convention, if there is a pkginfp['artifacts']['main'], use it automatically.
	if not gosum_data and not gosum_path and 'artifacts' in pkginfo and isinstance(pkginfo['artifacts'], dict) and 'main' in pkginfo['artifacts']:
		src_artifact = pkginfo['artifacts']['main']

	if src_artifact:
		await src_artifact.ensure_fetched()
		src_artifact.extract()
		src_dir = glob.glob(os.path.join(src_artifact.extract_path, src_dir_glob))[0]
		gosum_path = os.path.join(src_dir, "go.sum")
		if not os.path.exists(gosum_path):
			subprocess.Popen(["go", "mod", "download"], cwd=src_dir).wait()
		gosum, pkginfo["gosum_bundle"].mod_attrs_list = gen_gosum(gosum_path=gosum_path)
		src_artifact.cleanup()
	elif gosum_path:
		gosum, pkginfo["gosum_bundle"].mod_attrs_list = gen_gosum(gosum_path=gosum_path)
	else:
		gosum, pkginfo["gosum_bundle"].mod_attrs_list = gen_gosum(gosum_data=gosum_data)
	pkginfo["gosum"] = gosum
	# Create a hash of the list of go modules for brevity
	master_gosum = hashlib.sha512(gosum.encode("utf-8")).hexdigest()
	pkginfo["gosum_bundle"].key = AttrDict({"catpkg": f"{pkginfo['cat']}/{pkginfo['name']}", "version": pkginfo["version"], "gosum_hash": master_gosum})
	pkginfo["gosum_bundle"].final_name = f"{pkginfo['name']}-{pkginfo['version']}-funtoo-go-bundle-{pkginfo['gosum_bundle'].key.gosum_hash}.tar.gz"
	my_archive, metadata = hub.Archive.find(key=pkginfo["gosum_bundle"].key, final_name=pkginfo["gosum_bundle"].final_name)
	if my_archive is None:
		my_archive = await create_gosum_archive(hub, pkginfo)
	if "artifacts" not in pkginfo:
		pkginfo["artifacts"] = {}
	if isinstance(pkginfo["artifacts"], list):
		pkginfo["artifacts"].append(my_archive)
	elif isinstance(pkginfo["artifacts"], dict):
		pkginfo["artifacts"]["go_bundle"] = my_archive
	else:
		raise ValueError(f"Unrecognized type for pkginfo['artifacts']: {type(pkginfo['artifacts'])}")


async def generate_gosum_from_artifact(src_artifact, src_dir_glob="*"):
	"""
	This method, when passed an Artifact, will fetch the artifact, extract it, look in the directory
	``src_dir_glob`` (a glob specifying the name of the source directory within the extracted files
	which contains ``go.sum`` -- you can also specify sub-directories as part of this glob), and
	will then parse ``go.sum`` for package names, and then generate a list of artifacts for each
	module discovered. This list of new artifacts will be returned as a list. In the case there is no
	``go.sum`` present in the artifact, ``go mod download`` will be run to generate one.
	"""
	await src_artifact.fetch()
	src_artifact.extract()
	src_dir = glob.glob(os.path.join(src_artifact.extract_path, src_dir_glob))[0]
	gosum_path = os.path.join(src_dir, "go.sum")
	if not os.path.exists(gosum_path):
		subprocess.Popen(["go", "mod", "download"], cwd=src_dir).wait()
	artifacts = await get_gosum_artifacts(gosum_path)
	src_artifact.cleanup()
	return artifacts
