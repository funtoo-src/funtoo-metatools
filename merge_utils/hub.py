#!/usr/bin/env python3
import asyncio
import os
import importlib.util
import threading
import types


class PluginDirectory:
	def __init__(self, hub, path, init_kwargs=None):
		self.path = path
		self.hub = hub
		self.init_done = False  # This means that we tried to run init.py if one existed.
		self.loaded = False  # This means the plugin directory has been fully loaded and initialized.
		self.plugins = {}
		self.init_kwargs = init_kwargs
		# I'm testing this -- it's probably best to make sure we do all init at the very beginning.
		self.do_dir_init()

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
			print(f"Loading init from {init_path}")
			# Load "init.py" plugin and also pass init_kwargs which will get passed to the __init__() method.
			self.plugins["init"] = self.hub.load_plugin(init_path, "init", init_kwargs=self.init_kwargs)
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

	def __getattr__(self, plugin_name):
		if not self.loaded:
			self.load()
		if plugin_name not in self.plugins:
			raise AttributeError(f"Plugin {plugin_name} not found.")
		return self.plugins[plugin_name]


class Hub:
	def __init__(self, lazy=True):
		self.root_dir = os.path.normpath(os.path.join(os.path.realpath(__file__), "../../"))
		self.paths = {}
		self.lazy = lazy
		self._thread_ctx = threading.local()

		# In Python, threads and asyncio event loops are connected. Python asyncio has the concept of the
		# "current asyncio loop", and this current event loop is set per OS thread. So to properly handle
		# this, it is best to use thread-local storage here -- so we are properly dealing with the
		# sophistication of asyncio and how event loops are mapped.

		try:
			self._thread_ctx.loop = asyncio.get_running_loop()
		except RuntimeError:
			self._thread_ctx.loop = asyncio.new_event_loop()
			asyncio.set_event_loop(self._thread_ctx.loop)

	@property
	def THREAD_CTX(self):
		return self._thread_ctx

	@property
	def LOOP(self):
		"""
		This is the best way to get the current event loop for the current thread using subpop.

		Internally, we use thread-local storage so that everything in this OS thread gets the same result,
		but different OS threads will have their own event loop.
		"""
		loop = getattr(self._thread_ctx, "loop", None)
		if loop is None:
			loop = self._thread_ctx.loop = asyncio.new_event_loop()
		return loop

	def add(self, path, name=None, **init_kwargs):
		if name is None:
			name = os.path.basename(path)
		pdir = os.path.join(self.root_dir, path)
		if not os.path.isdir(pdir):
			raise FileNotFoundError(f"Plugin directory {pdir} not found or not a directory.")
		self.paths[name] = PluginDirectory(self, pdir, init_kwargs=init_kwargs)
		if not self.lazy:
			self.paths[name].load()

	def load_plugin(self, path, name, init_kwargs=None):
		spec = importlib.util.spec_from_file_location(name, path)
		if spec is None:
			raise FileNotFoundError(f"Could not find plugin: {path}")
		mod = importlib.util.module_from_spec(spec)
		spec.loader.exec_module(mod)
		# inject hub into plugin so it's available:
		mod.hub = self
		init_func = getattr(mod, "__init__", None)
		if init_func is not None and isinstance(init_func, types.FunctionType):
			if init_kwargs is None:
				init_func()
			else:
				# pass what was sent to hub.add("foo", blah=...) as kwargs to __init__() in the init.py.
				init_func(**init_kwargs)
		return mod

	def __getattr__(self, name):
		if name not in self.paths:
			raise AttributeError(f"{name} not found on hub.")
		return self.paths[name]
