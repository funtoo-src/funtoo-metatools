import setuptools

with open("README.rst", "r") as fh:
	long_description = fh.read()

setuptools.setup(
	name="funtoo-metatools",
	version="0.9.6",
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
		"subpop >= 0.4.2",
		"beautifulsoup4",
		"dict_toolbox",
		"httpx",
		"Jinja2 >= 3",
		"packaging",
		"pymongo",
		"PyYAML",
		"tornado",
		"toml",
		"xmltodict",
	],
	packages=setuptools.find_packages(),
	package_data={"": ["*.tmpl"]},
)

# vim: ts=4 sw=4 noet
