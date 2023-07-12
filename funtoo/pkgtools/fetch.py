#!/usr/bin/env python3
import asyncio
import re
from datetime import timedelta, datetime

from metatools.fastpull.spider import FetchError, FetchRequest, ContentNotModified
from metatools.fetch_cache import CacheMiss

"""
This sub implements high-level fetching pkgtools.model.log.c. Not the lower-level HTTP stuff. Things involving
retrying, using our fetch cache, etc.
"""

import dyne.org.funtoo.metatools.pkgtools as pkgtools


async def fetch_harness(fetch_method, url, refresh_interval=None, **kwargs):
	"""
	This method is used to execute any fetch-related method, and will handle all the aspects of reading from and
	writing to the fetch cache, as needed, based on the current fetch policy. Arguments include ``fetch_method``
	which is the actual method used to fetch -- the function itself -- which should be a function or method that
	accepts a single non-keyword argument of the URL to fetch, and it should return the result of the fetch
	if successful, or raise FetchError on failure.

	The parameter ``url`` is of course the URL to fetch, and ``refresh_interval`` is a timedelta which specifies the
	minimum interval before updating the cached resource. This is useful for packages (like the infamous vim)
	that may get updated too frequently otherwise. Pass ``refresh_interval=timedelta(days=7)`` to only allow for
	updates to the cached metadata every 7 days. Default is None which means to refresh at will (no restrictions
	to frequency.)

	This function will 'fall back' to the cache if the live fetch fails (and is thus more resilient).
	"""
	pkgtools.model.log.debug(f"refresh interval in fetch_harness: {refresh_interval}")
	attempts = 0
	fail_reason = None
	if refresh_interval is None:
		if pkgtools.model.fetch_cache_interval is not None:
			# pkgtools.model.fetch_cache_interval defaults to 15 minutes and will allow caching of stuff within that window
			# by default unless overridden by the doit --immediate option, or if there was an explicit refresh interval passed
			# to this function.
			refresh_interval = pkgtools.model.fetch_cache_interval
		else:
			refresh_interval = timedelta(minutes=15)

	# This may have "is_json", "encoding":
	key_dict = kwargs.copy()
	key_dict.update({
		"method_name": fetch_method.__name__,
		"url": url
	})

	while attempts < pkgtools.model.fetch_attempts:
		attempts += 1
		try:
			if fetch_method == "get_page" or refresh_interval is not None:
				# Let's see if we should use an 'older' resource that we don't want to refresh as often.

				# This call will return our cached resource if it's available and refresh_interval hasn't yet expired, i.e.
				# it is not yet 'stale'.
				try:
					result = await pkgtools.model.fetch_cache.read(
						key_dict=key_dict,
						refresh_interval=refresh_interval
					)
					if refresh_interval:
						pkgtools.model.log.info(f'Fetched {url} (cached, refresh_interval: {refresh_interval})')
					else:
						pkgtools.model.log.info(f'Fetched {url} (cached)')
					return result["body"]
				except CacheMiss:
					# We'll continue and attempt a live fetch of the resource...
					pass
			result = await fetch_method(url, **kwargs)
			await pkgtools.model.fetch_cache.write(key_dict=key_dict, body=result)
			return result
		except FetchError as e:
			if e.retry and attempts + 1 < pkgtools.model.fetch_attempts:
				pkgtools.model.log.error(f"Fetch method {fetch_method.__name__}: {e.msg}; retrying...")
				continue
			# TODO: I need a lot more info here -- if something failed -- why? this is important for IPv6 debug
			# if we got here, we are on our LAST retry attempt or retry is False:
			pkgtools.model.log.warning(f"Unable to retrieve {url}... trying to used cached version instead...")
			# TODO: these should be logged persistently so they can be investigated.
			try:
				# TODO: add kwargs here....
				got = await pkgtools.model.fetch_cache.read(key_dict=key_dict)
				return got["body"]
			except CacheMiss as ce:
				# raise original exception
				raise e
		except asyncio.CancelledError as e:
			raise FetchError(url, f"Fetch of {url} cancelled.")

	# If we've gotten here, we've performed all of our attempts to do live fetching.
	try:
		result = await pkgtools.model.fetch_cache.read(key_dict=key_dict)
		return result["body"]
	except CacheMiss:
		await pkgtools.model.fetch_cache.record_fetch_failure(key_dict=key_dict, fail_reason=fail_reason)
		raise FetchError(
			url,
			f"Unable to retrieve {url} using method {fetch_method.__name__} either live or from cache as fallback.",
		)


def set_basic_auth(request: FetchRequest):
	"""
	Keyword arguments to get_page() GET requests for authentication to certain URLs based on configuration
	in ~/.autogen (YAML format.)
	"""
	if "authentication" in pkgtools.model.config:
		if request.hostname in pkgtools.model.config["authentication"]:
			auth_info = pkgtools.model.config["authentication"][request.hostname]
			request.set_auth(**auth_info)


async def really_get_page(url, encoding=None, is_json=False, cached_result=None):
	"""
	The purpose of this method is to REALLY attempt to fetch the page -- but also use our cached result to properly
	use If-Modified-Since and ETag -- and also handle writing to the fetch cache upon successful retrieval.

	This method will also handle RETRYING the fetch if it fails for some network-related reason.
	"""
	request = FetchRequest(url=url)
	set_basic_auth(request)

	# Pass existing ETag or Last-Modified (If-Modified-Since) to request if available:

	extra_headers = {}
	if cached_result:
		if "headers" in cached_result:
			if "ETag" in cached_result["headers"]:
				extra_headers["If-None-Match"] = cached_result["headers"]["ETag"]
			if "Last-Modified" in cached_result["headers"]:
				extra_headers["If-Modified-Since"] = cached_result["headers"]["Last-Modified"]

	attempts = 0
	while attempts < pkgtools.model.fetch_attempts:
		attempts += 1
		try:
			try:
				headers, result = await pkgtools.model.spider.http_fetch(request, encoding=encoding, is_json=is_json, extra_headers=extra_headers, allow_304=True)
			except ContentNotModified:
				result = cached_result["body"]
				headers = cached_result["headers"] if "headers" in cached_result else {}

			key_dict = {
				"method_name": "get_page",
				"url": url,
				"is_json": is_json
			}
			if encoding:
				key_dict["encoding"] = encoding

			# We will store ETag or Last-Modified headers from the response:

			out_headers = {}
			for head_name in ["ETag", "Last-Modified"]:
				if head_name in headers:
					out_headers[head_name] = headers[head_name]
			if out_headers:
				key_dict["headers"] = out_headers
			await pkgtools.model.fetch_cache.write(key_dict=key_dict, body=result)
			return result

		except FetchError as e:
			if e.retry and attempts + 1 < pkgtools.model.fetch_attempts:
				pkgtools.model.log.error(f"Fetch method get_page: {e.msg}; retrying...")
				continue
			# if we got here, we are on our LAST retry attempt or retry is False:
			pkgtools.model.log.warning(f"Unable to retrieve {url}... trying to used cached version instead...")
			# TODO: these should be logged persistently so they can be investigated.
			if cached_result:
				return cached_result["body"]
			else:
				raise e
		except asyncio.CancelledError as e:
			raise FetchError(url, f"Fetch of {url} cancelled.")


async def get_page(fetchable, encoding=None, refresh_interval=None, is_json=False):
	"""
	This method is responsible for implementing the higher-level HTTP resource retrieval logic, which includes
	potentially short-circuiting the fetch and simply returning a cached result without doing HTTP at all, if
	we have a suitable cached version locally.

	If we don't have a cached version locally, we will do a REAL HTTP fetch using ``really_get_page()``, above.
	"""
	pkgtools.model.log.debug(
		f'get_page {fetchable}, refresh_interval={refresh_interval}, is_json={is_json}')
	if isinstance(fetchable, pkgtools.ebuild.Artifact):
		url = fetchable.url
	else:
		url = fetchable

	if refresh_interval is None:
		refresh_interval = pkgtools.model.fetch_cache_interval

	try:
		key_dict = {"method_name": "get_page", "url": url, "is_json": is_json}
		if encoding:
			key_dict["encoding"] = encoding
		cached_result = await pkgtools.model.fetch_cache.read(
			key_dict=key_dict
		)
	except CacheMiss:
		cached_result = None

	if cached_result and not pkgtools.model.immediate:

		# We already have a legitimate cached result that is within our refresh interval, so return it -- we do not
		# need to HTTP query for any updated resource:

		if datetime.utcnow() - cached_result["fetched_on"] <= refresh_interval:
			return cached_result['body']

	return await really_get_page(url, encoding=encoding, is_json=is_json, cached_result=cached_result)


async def get_response_headers(fetchable, refresh_interval=None):
	if isinstance(fetchable, pkgtools.ebuild.Artifact):
		fetchable = fetchable.url
	return await fetch_harness(
		pkgtools.model.spider.get_response_headers, fetchable, refresh_interval=refresh_interval
	)


async def get_response_filename(fetchable, refresh_interval=None):
	"""
	This method gets the response's filename without fetching its body.
	This is achieved by looking at the `Content-Disposition` header.
	If the `Content-Disposition` header is not set or if it doesn't contain the filename,
	then it will return `None`.
	"""
	if isinstance(fetchable, pkgtools.ebuild.Artifact):
		fetchable = fetchable.url
	headers = await pkgtools.model.spider.get_response_headers(fetchable,refresh_interval=refresh_interval)
	res = re.search(r"filename=\"?(\S+)\"?", headers.get("Content-Disposition", ""))
	return None if not res else res.group(1)


async def get_url_from_redirect(fetchable, refresh_interval=None):
	if isinstance(fetchable, pkgtools.ebuild.Artifact):
		fetchable = fetchable.url
	return await fetch_harness(
		pkgtools.model.spider.get_url_from_redirect, fetchable, refresh_interval=refresh_interval
	)

# vim: ts=4 sw=4 noet
