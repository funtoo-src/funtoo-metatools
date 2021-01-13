#!/usr/bin/env python3

import asyncio
import subprocess
import os
import threading
import sys
from asyncio import FIRST_EXCEPTION, Task
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor

import yaml
from yaml import safe_load
import logging

hub = None

"""
The `PENDING_QUE` will be built up to contain a full list of all the catpkgs we want to autogen in the full run
of 'doit'. We queue up everything first so that we have the ability to add QA checks, such as for catpkgs that
are defined multiple times and other errors that we should catch before processing begins. The work is organized
by the generator (pop plugin) that will be used to generate a list of catpkgs. Before we start, we have
everything organized so that we only need to call `execute_generator` once for each generator. It will start
work for all catpkgs in that generator, and wait for completion of this work before returning.

Similarly to how we queue up all work before we start, `ERRORS` will contain a list of all errors encountered
during processing.
"""

PENDING_QUE = []
ERRORS = []


SUB_FP_MAP_LOCK = asyncio.Lock()
SUB_FP_MAP = {}

"""
While it is possible for a `generate()` function to call the `generate()` method on a `BreezyBuild` directly,
in nearly all cases the `BreezyBuild`'s `push` method is called to queue it for processing. When `push` is
called, we want to start the `generate` method as an asyncio `Task` and then keep track of it so we can wait for
all of these `Tasks` to complete.

When these tasks are running, they are using a specific generator (pop plugin) and we want to be able to wait
for these tasks to complete after we have processed all the work for the generator. This is for two reasons --
so we can enforce a limit for the number of generators running at once, and so we do not exit prematurely while
generators have still not completed their work.

`BREEZYBUILD_TASKS` is configured to hold these tasks. They are organized by the 'sub-index', which is a string
name we use to reference the generator internally. That way we can segregate our tasks by generator, which is
important so each generator can wait for only its own tasks to complete.
"""

BREEZYBUILDS_PENDING = defaultdict(list)
BREEZYBUILD_TASKS_ACTIVE = defaultdict(list)
BREEZYBUILD_SUB_INDEX_HANDOFF = {}


def generate_manifests():
	"""
	Once auto-generation is complete, this function will write all stored Manifest data to disk. We do this after
	autogen completes so we can ensure that all necessary ebuilds have been created and we can ensure that these are
	written once for each catpkg, rather than written as each individual ebuild is autogenned (which would create a
	race condition writing to each Manifest file.)
	"""
	for manifest_file, manifest_lines in hub.MANIFEST_LINES.items():
		manifest_lines = sorted(list(manifest_lines))
		with open(manifest_file, "w") as myf:
			pos = 0
			while pos < len(manifest_lines):
				myf.write(manifest_lines[pos])
				pos += 1
		logging.debug(f"Manifest {manifest_file} generated.")


def queue_all_indy_autogens():
	"""
	This will find all independent autogens and queue them up in the pending queue.
	"""
	s, o = subprocess.getstatusoutput("find %s -iname autogen.py 2>&1" % hub.CONTEXT.start)
	files = o.split("\n")
	for file in files:
		file = file.strip()
		if not len(file):
			continue

		subpath = os.path.dirname(file)
		if subpath.endswith("metatools"):
			continue

		pkg_name = file.split("/")[-2]
		pkg_cat = file.split("/")[-3]

		PENDING_QUE.append(
			{
				"gen_path": subpath,
				"generator_sub_path": subpath,
				"template_path": os.path.join(subpath, "templates"),
				"pkginfo_list": [{"name": pkg_name, "cat": pkg_cat}],
			}
		)
		logging.debug(f"Added to queue of pending autogens: {PENDING_QUE[-1]}")


async def gather_pending_tasks(task_list, throw=False):
	"""
	This function collects completed asyncio coroutines, catches any exceptions recorded during their execution.
	"""
	exceptions = []
	results = []
	cur_tasks = task_list
	if not len(cur_tasks):
		return [], []
	while True:
		done_list, cur_tasks = await asyncio.wait(cur_tasks, return_when=FIRST_EXCEPTION)
		for done_item in done_list:
			if throw:
				result = done_item.result()
				results.append(result)
			else:
				try:
					result = done_item.result()
					results.append(result)
				except Exception as e:
					exceptions.append((e, sys.exc_info()))
		if not len(cur_tasks):
			break
	return results, exceptions


def init_pkginfo_for_package(defaults=None, base_pkginfo=None, template_path=None, gen_path=None):
	"""
	This function generates the final pkginfo that is passed to the generate() function in the generator sub
	for each catpkg being generated.

	we create the pkginfo data that gets passed to generate. You can see that it can come from multiple places:

	1. A generator sub can define a `GLOBAL_DEFAULTS` dictionary that contains global settings. These are
	   set first.

	2. Then, any defaults that are provided to us, which have come from the `defaults:` section of the
	   autogen.yaml are supplied. (`defaults`, below.)

	3. Next, `cat` and `name` settings calculated based on the path of the `autogen.py`, or the settings that
	   come from the package-specific part of the `autogen.yaml` are added on top. (`base_pkginfo`, below.)
	"""
	glob_defs = getattr(hub.THREAD_CTX.sub, "GLOBAL_DEFAULTS", {})
	pkginfo = glob_defs.copy()
	if defaults is not None:
		pkginfo.update(defaults)
	pkginfo.update(base_pkginfo)
	if template_path:
		pkginfo["template_path"] = template_path

	# Now that we have wrapped the generate method, we need to start it as an asyncio task and then we will wait
	# for all our generate() calls to complete, outside this for loop.

	# This is the path where the autogen lives. Either the autogen.py or the autogen.yaml:
	common_prefix = os.path.commonprefix([hub.CONTEXT.root, gen_path])
	path_from_root = gen_path[len(common_prefix) :].lstrip("/")
	pkginfo["gen_path"] = f"${{REPODIR}}/{path_from_root}"
	return pkginfo


async def execute_generator(
	generator_sub_path=None,
	generator_sub_name="autogen",
	template_path=None,
	defaults=None,
	pkginfo_list=None,
	gen_path=None,
):
	"""
	This function will return an async function that requires no arguments, that is ready to run in its own
	thread using run_async_adapter. This function will execute the full auto-generation for a particular
	generator/autogen.py and will wait until all of its asyncio tasks have completed before returning. This
	neatly allows an autogeneration for a sub/generator/autogen.py to be contained in its own thread, improving
	performance and allowing the use of thread-local storage to keep track of things specific to this autogen
	run.
	"""

	if generator_sub_path:
		# This is an individual autogen.py. First grab the "base sub" (map the path), and then grab the actual sub-
		# module we want by name.
		generator_sub = hub.load_plugin(f"{generator_sub_path}/{generator_sub_name}.py", generator_sub_name)
	else:
		# This is an official generator that is built-in to pkgtools:
		generator_sub = getattr(hub.generators, generator_sub_name)

	# The generate_wrapper wraps the call to `generate()` (in autogen.py or the generator) and performs setup
	# and post-tasks:

	async def generator_thread_task():
		assert id(asyncio.get_running_loop()) == id(hub.THREAD_CTX.loop)
		print(f"********************** Executing generator {generator_sub_name}")

		hub.THREAD_CTX.sub = generator_sub
		hub.THREAD_CTX.running_autogens = []
		hub.THREAD_CTX.running_breezybuilds = []

		# Generate some output to let the user know what we're doing:

		for base_pkginfo in pkginfo_list:
			pkginfo = init_pkginfo_for_package(
				defaults=defaults, base_pkginfo=base_pkginfo, template_path=template_path, gen_path=gen_path
			)
			if "version" in pkginfo and pkginfo["version"] != "latest":
				print(f"autogen: {pkginfo['cat']}/{pkginfo['name']}-{pkginfo['version']}")
			else:
				print(f"autogen: {pkginfo['cat']}/{pkginfo['name']} (latest)")
			logging.debug(f"Using the following pkginfo for auto-generation: {pkginfo}")

			# Any .push() calls on BreezyBuilds will cause new tasks for those to be appended to
			# hub.THREAD_CTX.running_breezybuilds. This will happen during this task execution:

			async def gen_wrapper(pkginfo):
				# For now, all generate() methods in autogens are expecting the hub.
				await hub.THREAD_CTX.sub.generate(hub, **pkginfo)
				return pkginfo

			hub.THREAD_CTX.running_autogens.append(Task(gen_wrapper(pkginfo)))

		results, errors = await gather_pending_tasks(hub.THREAD_CTX.running_autogens)
		# This will return the pkginfo dict used for the autogen, if you want to inspect it

		results, errors = await gather_pending_tasks(hub.THREAD_CTX.running_breezybuilds)
		# This will return the BreezyBuild object if you want to inspect it for debugging:

	return generator_thread_task


def parse_yaml_rule(package_section=None):

	pkginfo_list = []

	if type(package_section) == str:

		# A simple '- pkgname' one-line format:
		#
		# - foobar
		#
		pkginfo_list.append({"name": package_section})

	elif type(package_section) == dict:

		# A more complex format, where the package has sub-settings.
		#
		# - foobar:
		#     val1: blah
		#
		# { 'pkgname' : { 'value1' : 'foo', 'value2' : 'bar' } }

		# Remove extra singleton outer dictionary (see format above)

		package_name = list(package_section.keys())[0]
		pkg_section = list(package_section.values())[0]
		pkg_section["name"] = package_name

		# This is even a more complex format, where we have sub-sections based on versions of the package,
		# each with their own settings:
		#
		# - foobar:
		#     versions:
		#       1.2.4:
		#         val1: blah
		#       latest:
		#         val1: bleeeeh

		if type(pkg_section) == dict and "versions" in pkg_section:
			versions_section = pkg_section["versions"]
			for version, v_pkg_section in versions_section.items():
				v_pkginfo = {"name": package_name}
				v_pkginfo.update(v_pkg_section)
				v_pkginfo["version"] = version
				pkginfo_list.append(v_pkginfo)
		else:
			pkginfo_list.append(pkg_section)

	return pkginfo_list


def queue_all_yaml_autogens():

	"""
	This function finds all autogen.yaml files in the repository and adds work to the `PENDING_QUE` (via calls
	to `parse_yaml_rule`.) This queues up all generators to execute.
	"""

	s, o = subprocess.getstatusoutput("find %s -iname autogen.yaml 2>&1" % hub.CONTEXT.start)
	files = o.split("\n")

	for file in files:
		file = file.strip()
		if not len(file):
			continue
		yaml_base_path = os.path.dirname(file)

		with open(file, "r") as myf:
			for rule_name, rule in safe_load(myf.read()).items():

				if "defaults" in rule:
					defaults = rule["defaults"]
				else:
					defaults = {}

				if "generator" in rule:
					sub_path = os.path.join(yaml_base_path, "generators")
					sub_name = rule["generator"]
					if os.path.exists(os.path.join(sub_path, rule["generator"] + ".py")):
						# We found a generator in a "generators" directory next to the autogen.yaml that contains the
						# generator.
						logging.debug(f"Found generator {sub_name} in local tree.")
					else:

						sub_path = None
						logging.debug(f"Using built-in generator {sub_name}.")
				else:
					# Fallback: Use an ad-hoc 'generator.py' generator in the same dir as autogen.yaml:
					sub_name = "generator"
					sub_path = yaml_base_path

				pkginfo_list = []
				for package in rule["packages"]:
					pkginfo_list += parse_yaml_rule(package_section=package)
				PENDING_QUE.append(
					{
						"gen_path": yaml_base_path,
						"generator_sub_name": sub_name,
						"generator_sub_path": sub_path,
						"template_path": os.path.join(yaml_base_path, "templates"),
						"defaults": defaults,
						"pkginfo_list": pkginfo_list,
					}
				)
				logging.debug(f"Added to queue of pending autogens: {PENDING_QUE[-1]}")


async def execute_all_queued_generators():
	futures = []
	loop = asyncio.get_running_loop()
	with ThreadPoolExecutor(max_workers=8) as executor:
		while len(PENDING_QUE):
			task_args = PENDING_QUE.pop(0)
			async_func = await execute_generator(**task_args)
			future = loop.run_in_executor(executor, hub.pkgtools.thread.run_async_adapter, async_func)
			futures.append(future)

		results, exceptions = await gather_pending_tasks(futures)


async def start(start_path=None, out_path=None, fetcher=None):

	"""
	This method will start the auto-generation of packages in an ebuild repository.
	"""
	hub.FETCHER = fetcher
	hub.pkgtools.repository.set_context(start_path=start_path, out_path=out_path)
	hub.add("funtoo/generators")
	queue_all_indy_autogens()
	queue_all_yaml_autogens()
	await execute_all_queued_generators()
	generate_manifests()
	return ERRORS


# vim: ts=4 sw=4 noet
