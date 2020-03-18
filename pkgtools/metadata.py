#!/usr/bin/env python3

import os
from json import dumps, loads

def get_metadata(hub, path):
	try:
		def object_hook(obj):
			if "$type" in obj and obj["$type"] == "Artifact":
				print("LOADED Artifact from metadata!")
				return hub.pkgtools.ebuild.Artifact(metadata=obj)
			else:
				return obj
		metafile = os.path.join(path, "metadata.json")
		if not os.path.exists(metafile):
			raise hub.pkgtools.ebuild.BreezyError("Metadata %s does not exist." % metafile)
		with open(metafile, "r") as myf:
			metadata = loads(myf.read(), object_hook=object_hook)
			return metadata
	except (PermissionError, IOError) as e:
		raise hub.pkgtools.ebuild.BreezyError("Unable to read metadata: %s" % e)


async def write_metadata(hub, path, metadata):
	def encoder(data):
		if type(data) == hub.pkgtools.ebuild.Artifact:
			return data.as_metadata()
		else:
			return data

	with open(os.path.join(path, "metadata.json"), "w") as myf:
		myf.write(dumps(metadata, default=encoder))
