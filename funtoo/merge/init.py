from merge_utils.config import Configuration
import pymongo
from pymongo import MongoClient

hub = None


def __init__(release=None, **kwargs):
	hub.RELEASE = release
	hub.MERGE_CONFIG = Configuration(**kwargs)

	mc = MongoClient()
	dd = hub.DEEPDIVE = mc.metatools.deepdive
	dd.create_index("atom")
	dd.create_index([("kit", pymongo.ASCENDING), ("category", pymongo.ASCENDING), ("package", pymongo.ASCENDING)])
	dd.create_index("catpkg")
	dd.create_index("relations")
	dd.create_index("md5")
	dd.create_index("files.name", partialFilterExpression={"files": {"$exists": True}})

	di = hub.DISTFILE_INTEGRITY = mc.metatools.distfile_integrity
	di.create_index([("category", pymongo.ASCENDING), ("package", pymongo.ASCENDING), ("distfile", pymongo.ASCENDING)])
