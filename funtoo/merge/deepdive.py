#!/usr/bin/python3

"""
The DeepDive database is designed to get wiped and re-loaded to contain only the metadata for all ebuilds processed
in the last merge-kits run.

In contrast, the Distfile Integrity database is intended to be persistent and store cryptographic hashes related
to distfiles used by catpkgs. This allows us to autogen ebuilds without having the actual distfile present, and ensures
that distfile hashes don't magically change if a newly-fetched distfile has been modified upstream.

When using the fastpull database with autogen, we need the Distfile Integrity Database to retrieve existing
distfiles that have been stored in fastpull. We need a way to map the distfile filename (which is not stored
in fastpull) to a SHA1 that exists in fastpull. The Distfile Integrity Database provides this mapping.

The Distfile Integrity database associates distfile file names with catpkgs. The catpkg is the 'namespace' for
any files. Additionally, when run in production mode, the release, kit and branch are recorded in the database.

The one challenge that appears necessary to resolve with the Distfile Integrity Database is that we can potentially have
multiple 'doit' processes accessing it at the same time and reading and writing to it, due to 'merge-kits'
multi-threaded architecture.

However, this is not needed -- due to the design of 'merge-kits', and the fact that 'doit'
will be running on a particular release, kit and branch, any reads and writes will not clobber one another, and thus
we don't need to arbitrate/lock access to the Distfile Integrity DB. The Architecture makes it safe.
"""

hub = None


def get_distfile_integrity(catpkg=None, distfile=None):
	return hub.DISTFILE_INTEGRITY.find_one({"catpkg": catpkg, "distfile": distfile})


def store_distfile_integrity(catpkg, final_name, final_data, **kwargs):
	"""
	Store something in the distfile integrity database. This method is not thread-safe so you should call it from the
	main thread of 'doit' and not a sub-thread.
	"""
	out = {"catpkg": catpkg, "distfile": final_name, "final_data": final_data}
	for extra in "release", "kit", "branch":
		if extra in kwargs:
			out[extra] = kwargs[extra]

	hub.DISTFILE_INTEGRITY.insert_one(out)
