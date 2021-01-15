import setuptools

with open("README.rst", "r") as fh:
	long_description = fh.read()

setuptools.setup(
	name="funtoo-metatools",
	version="0.5.5",
	author="Daniel Robbins",
	author_email="drobbins@funtoo.org",
	description="Funtoo framework for auto-creation of ebuilds.",
	long_description=long_description,
	long_description_content_type="text/x-rst",
	url="https://code.funtoo.org/bitbucket/users/drobbins/repos/funtoo-metatools/browse",
	scripts=["bin/doit"],
	classifiers=[
		"Programming Language :: Python :: 3",
		"License :: OSI Approved :: Apache Software License",
		"Operating System :: POSIX :: Linux",
	],
	python_requires=">=3.7",
	install_requires=[
		"sphinx_funtoo_theme",
		"Jinja2",
		"xmltodict",
		"aiodns",
		"aiofiles",
		"aiohttp",
		"pymongo",
		"tornado",
		"toml",
		"beautifulsoup4",
		"dict_toolbox",
	],
	#   320  sudo emerge -av dict-toolbox
	#   322  sudo emerge -av aiofiles
	#   333  sudo emerge -av pymongo
	#   335  sudo emerge -av aiohttp
	#   337  sudo emerge -av tornado
	#   338  sudo emerge -av www-servers/tornado
	#   339  sudo emerge -av beautifulsoup
	#   341  sudo emerge -av xmltodict
	#   344  sudo emerge -av jinja
	#   346  sudo emerge -av aiodns
	packages=setuptools.find_packages(),
	package_data={"": ["*.tmpl"]},
)
