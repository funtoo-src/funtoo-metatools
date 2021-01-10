#!/usr/bin/env python3

from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import cpu_count

hub = None


def __init__():
	hub.CPU_BOUND_EXECUTOR = ThreadPoolExecutor(max_workers=cpu_count())


def get_threadpool():
	return ThreadPoolExecutor(max_workers=cpu_count())


def run_async_adapter(corofn, *args, **kwargs):
	"""
	Use this method to run an asynchronous worker within a ThreadPoolExecutor.
	Without this special wrapper, this normally doesn't work, and the
	ThreadPoolExecutor will not allow async calls.  But with this wrapper, our
	worker and its subsequent calls can be async.
	"""
	return hub.LOOP.run_until_complete(corofn(*args, **kwargs))


# vim: ts=4 sw=4 noet
