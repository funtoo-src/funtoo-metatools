#!/usr/bin/env python3

import os
import importlib.util
import types


class PluginDirectory:
	def __init__(self, hub, path):
		self.path = path
		self.hub = hub
		self.init_done = False  # This means that we tried to run init.py if one existed.
		self.loaded = False  # This means the plugin directory has been fully loaded and initialized.
		self.plugins = {}

	def load_plugin(self, plugin_name):
		"""
		This allows a plugin to be explicitly loaded, which is handy if you are using lazy loading (load on first
		reference to something in a plugin) but your first interaction with it
		"""
		self.do_dir_init()
		if self.loaded:
			if plugin_name not in self.plugins:
				raise IndexError(f"Unable to find plugin {plugin_name}.")
		else:
			self.plugins[plugin_name] = self.hub.load_plugin(os.path.join(self.path, plugin_name + ".py"), plugin_name)

	def do_dir_init(self):
		if self.init_done:
			return
		init_path = os.path.join(self.path, "init.py")
		if os.path.exists(init_path):
			self.plugins["init"] = self.hub.load_plugin(init_path, "init")
		self.init_done = True

	def load(self):
		self.do_dir_init()
		for item in os.listdir(self.path):
			if item in ["__init__.py", "init.py"]:
				continue
			if item.endswith(".py"):
				plugin_name = item[:-3]
				if plugin_name not in self.plugins:
					self.plugins[plugin_name] = self.hub.load_plugin(os.path.join(self.path, item), plugin_name)
		self.loaded = True

	def __getattr__(self, item):
		if not self.loaded:
			self.load()
		if item not in self.plugins:
			raise AttributeError(f"{item} not found.")
		return self.plugins[item]


class Hub:
	def __init__(self, lazy=True):
		self.root_dir = os.path.normpath(os.path.join(os.path.realpath(__file__), "../../"))
		self.paths = {}
		self.lazy = lazy

	def add(self, path, name=None):
		if name is None:
			name = os.path.basename(path)
		self.paths[name] = PluginDirectory(self, os.path.join(self.root_dir, path))
		if not self.lazy:
			self.paths[name].load()

	def load_plugin(self, path, name):
		print(f"Loading {path}")
		spec = importlib.util.spec_from_file_location(name, path)
		if spec is None:
			raise FileNotFoundError(f"Could not find plugin: {path}")
		mod = importlib.util.module_from_spec(spec)
		spec.loader.exec_module(mod)
		# inject hub into plugin so it's available:
		mod.hub = self
		init_func = getattr(mod, "__init__", None)
		if init_func is not None and isinstance(init_func, types.FunctionType):
			init_func()
		return mod

	def __getattr__(self, name):
		if name not in self.paths:
			raise AttributeError(f"{name} not found on hub.")
		return self.paths[name]


if __name__ == "__main__":
	hub = Hub()
	hub.add("modules/funtoo/pkgtools", name="pkgtools")
	hb = hub.pkgtools.foobar.HubbaBubba()
