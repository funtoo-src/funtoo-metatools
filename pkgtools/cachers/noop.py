#!/usr/bin/env python3

def __init__(hub):
    pass


async def record_fetch_success(hub, method_name, url):
    pass


async def fetch_cache_write(hub, method_name, url, result):
    pass


async def fetch_cache_read(hub, method_name, url, max_age=None):
    pass


async def record_fetch_failure(hub, method_name, url):
    pass


async def metadata_cache_write(hub, repo_name, branch, catpkg, metadata):
    pass


async def metadata_cache_read(hub, repo_name, branch, catpkg):
    pass