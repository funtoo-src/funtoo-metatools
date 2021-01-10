from merge_utils.config import Configuration

hub = None


def __init__(release=None, **kwargs):
	hub.RELEASE = release
	hub.MERGE_CONFIG = Configuration(**kwargs)
