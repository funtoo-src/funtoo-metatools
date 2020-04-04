#!/usr/bin/env python3
import asyncio
import subprocess
import os
import logging
import traceback
from yaml import safe_load

def generate_manifests(hub):
	for manifest_file, manifest_lines in hub.MANIFEST_LINES.items():
		manifest_lines = sorted(list(manifest_lines))
		with open(manifest_file, "w") as myf:
			pos = 0
			while pos < len(manifest_lines):
				if pos != 0:
					myf.write('\n')
				myf.write(manifest_lines[pos])
				pos += 1
		logging.debug(f"Manifest {manifest_file} generated.")


async def generate_individual_autogens(hub):
	"""
	This method finds individual autogen.py files in the current repository path and runs them all.
	"""
	s, o = subprocess.getstatusoutput("find %s -iname autogen.py 2>&1" % hub.OPT.pkgtools['start_path'])
	files = o.split('\n')
	for file in files:
		file = file.strip()
		if not len(file):
			continue
		subpath = os.path.dirname(file)
		if subpath.endswith("pkgtools"):
			continue
		hub.pop.sub.add(static=subpath, subname="my_catpkg")

		# TODO: pass repo_name as well as branch to the generate method below:

		pkg_name = file.split("/")[-2]
		pkg_cat = file.split("/")[-3]
		try:
			await hub.my_catpkg.autogen.generate(name=pkg_name, cat=pkg_cat, path=subpath)
		except hub.pkgtools.fetch.FetchError as fe:
			logging.error(fe.msg)
			continue
		except hub.pkgtools.ebuild.BreezyError as be:
			logging.error(be.msg)
			continue
		except Exception as e:
			logging.error("Encountered problem in autogen script: \n\n" + traceback.format_exc())
			continue
		# we need to wait for all our pending futures before removing the sub:
		await hub.pkgtools.ebuild.parallelize_pending_tasks()
		hub.pop.sub.remove("my_catpkg")


async def process_yaml_rule(hub, generator_sub, package=None, defaults=None, subpath=None):
	"""
	This method takes a single YAML rule that we've extracted from an autogen.yaml file,
	loads the appropriate generator, and uses it to generate (probably) a bunch of catpkgs.
	"""
	pkginfo = generator_sub.GLOBAL_DEFAULTS.copy()
	pkginfo.update(defaults)
	pkginfo['template_path'] = os.path.join(subpath, "templates")
	pkginfo['path'] = subpath

	if type(package) == str:
		# simple '- pkgname' format.
		pkginfo['name'] = package
		await generator_sub.generate(**pkginfo)
	elif type(package) == dict:

		# more complex format.
		# if any sub-arguments are specified with the package, we get it in this format:
		# { 'pkgname' : { 'value1' : 'foo', 'value2' : 'bar' } }

		pkginfo['name'] = list(package.keys())[0]
		pkg_section = list(package.values())[0]

		if type(pkg_section) == list:
			# looks like this: [{'versions': [{'2.5.1': {'python_compat': 'python2_7 python3_{6,7,8} pypy3'}}, {'latest': {'python_compat': 'python3_{6,7,8} pypy3'}}]}]
			# we have multiple variants of this package
			versions_section = pkg_section[0]
			assert list(versions_section.keys())[0] == 'versions'

			for version_dict in list(versions_section.values())[0]:
				version = list(version_dict.keys())[0]
				v_pkg_section = list(version_dict.values())[0]
				# process each pkg
				v_pkginfo = pkginfo.copy()
				v_pkginfo['version'] = version
				v_pkginfo.update(v_pkg_section)
				await generator_sub.generate(**v_pkginfo)
		else:
			pkginfo.update(pkg_section)
			await generator_sub.generate(**pkginfo)

	await hub.pkgtools.ebuild.parallelize_pending_tasks()


async def generate_yaml_autogens(hub):
	"""
	This method finds autogen.yaml files in the repository and executes them. This provides a mechanism
	to perform autogeneration en-masse without needing to have individual autogen.py files all over the
	place.

	Currently supported in the initial implementation are autogen.yaml files existing in *category*
	directories.
	"""
	s, o = subprocess.getstatusoutput("find %s -iname autogen.yaml 2>&1" % hub.OPT.pkgtools['start_path'])
	files = o.split('\n')

	pending_tasks = []

	for file in files:
		file = file.strip()
		if not len(file):
			continue
		subpath = os.path.dirname(file)
		with open(file, 'r') as myf:
			for rule_name, rule in safe_load(myf.read()).items():
				if 'defaults' in rule:
					defaults = rule['defaults']
				else:
					defaults = {}
				generator_sub = getattr(hub.pkgtools.generators, rule['generator'])
				for package in rule['packages']:
					pending_tasks.append(process_yaml_rule(hub, generator_sub, package, defaults, subpath))

	await asyncio.gather(*pending_tasks)


async def start(hub, start_path=None, out_path=None):

	"""
	This method will start the auto-generation of packages in an ebuild repository.
	"""

	hub.pkgtools.repository.set_context(
		start_path if start_path is not None else hub.OPT.pkgtools.start_path,
		out_path=out_path if out_path is not None else hub.OPT.pkgtools.out_path, name=hub.OPT.pkgtools.name)

	await generate_individual_autogens(hub)
	await generate_yaml_autogens(hub)
	generate_manifests(hub)


# vim: ts=4 sw=4 noet