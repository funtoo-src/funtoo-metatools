#!/usr/bin/env python3
import logging
import sys
from asyncio import Semaphore
from collections import defaultdict
from urllib.parse import urlparse

import httpx

"""
This sub implements lower-level HTTP fetching logic, such as actually grabbing the data, sending the
proper headers and authentication, etc.
"""

import dyne.org.funtoo.metatools.pkgtools as pkgtools


async def acquire_host_semaphore(hostname):
	semaphores = getattr(hub.THREAD_CTX, "http_semaphores", None)
	if semaphores is None:
		semaphores = hub.THREAD_CTX.http_semaphores = defaultdict(lambda: Semaphore(value=8))
	return semaphores[hostname]


def get_fetch_headers():
	"""
	Headers to send for all HTTP requests.
	"""
	return {"User-Agent": "funtoo-metatools (support@funtoo.org)"}


def get_hostname(url):
	parsed_url = urlparse(url)
	return parsed_url.hostname


def get_auth_kwargs(hostname, url):
	"""
	Keyword arguments to ClientSession.get() for authentication to certain URLs based on configuration
	in ~/.autogen (YAML format.)
	"""
	kwargs = {}
	if "authentication" in pkgtools.model.AUTOGEN_CONFIG:
		if hostname in pkgtools.model.AUTOGEN_CONFIG["authentication"]:
			auth_info = pkgtools.model.AUTOGEN_CONFIG["authentication"][hostname]
			logging.warning(f"Using authentication (username {auth_info['username']}) for {url}")
			kwargs = {"auth": (auth_info["username"], auth_info["password"])}
	return kwargs


async def http_fetch_stream(url, on_chunk, retry=True, extra_headers=None):
	"""
	This is a streaming HTTP fetcher that will call on_chunk(bytes) for each chunk.
	On_chunk is called with literal bytes from the response body so no decoding is
	performed. A FetchError will be raised if any error occurs. If this function
	returns successfully then the download completed successfully.
	"""
	hostname = get_hostname(url)
	semi = await acquire_host_semaphore(hostname)
	rec_bytes = 0
	attempts = 0
	if retry:
		max_attempts = 3
	else:
		max_attempts = 1
	completed = False

	async with semi:
		while not completed and attempts < max_attempts:
			try:
				async with httpx.AsyncClient() as client:
					headers = get_fetch_headers()
					if extra_headers:
						headers.update(extra_headers)
					async with client.stream("GET", url, headers=headers, follow_redirects=True, **get_auth_kwargs(hostname, url)) as response:
						if response.status_code not in [200, 206]:
							if response.status_code in [400, 404, 410]:
								# These are legitimate responses that indicate that the file does not exist. Therefore, we
								# should not retry, as we should expect to get the same result.
								retry = False
							else:
								retry = True
							raise pkgtools.fetch.FetchError(url, f"HTTP fetch_stream Error {response.status_code}: {response.reason_phrase[:120]}", retry=retry)
						async for chunk in response.aiter_bytes():
							rec_bytes += len(chunk)
							if not chunk:
								completed = True
								break
							else:
								sys.stdout.write(".")
								sys.stdout.flush()
								on_chunk(chunk)
			except httpx.RequestError as e:
				print("Download failure")
				if attempts + 1 < max_attempts:
					attempts += 1
					print(f"Retrying after download failure... {e}")
					continue
				else:
					raise pkgtools.fetch.FetchError(url, f"{e.__class__.__name__}: {str(e)}")


async def http_fetch(url, encoding=None) -> str:
	"""
	This is a non-streaming HTTP fetcher that will properly convert the request to a Python string and return the entire
	content as a string.

	Use ``encoding`` if the HTTP resource does not have proper encoding and you have to set a specific encoding for string
	conversion. Normally, the encoding will be auto-detected and decoded for you.

	This method *will* return a FetchError if there was some kind of fetch failure, and this is used by the 'fetch cache'
	so this is important.
	"""
	hostname = get_hostname(url)
	semi = await acquire_host_semaphore(hostname)

	try:
		async with semi:
			async with httpx.AsyncClient() as client:
				print(f'Fetching data from {url}')
				response = await client.get(url, headers=get_fetch_headers(), **get_auth_kwargs(hostname, url), follow_redirects=True)
				if response.status_code != 200:
					if response.status_code in [400, 404, 410]:
						# No need to retry as the server has just told us that the resource does not exist.
						retry = False
					else:
						retry = True
					print(f"Fetch failure for {url}: {response.status_code} {response.reason_phrase[:40]}")
					raise pkgtools.fetch.FetchError(url, f"HTTP fetch Error: {url}: {response.status_code}: {response.reason_phrase[:40]}", retry=retry)
				if encoding:
					result = response.content.decode(encoding)
				else:
					result = response.text
				print(f'Fetched {url} {len(result)} bytes')
				return result
	except httpx.RequestError as re:
		raise pkgtools.fetch.FetchError(url, f"Could not connect to {url}: {repr(re)}", retry=False)


async def get_page(url, encoding=None):
	"""
	This function performs a simple HTTP fetch of a resource. The response is cached in memory,
	and a decoded Python string is returned with the result. FetchError is thrown for an error
	of any kind.

	Use ``encoding`` if the HTTP resource does not have proper encoding and you have to set
	a specific encoding. Normally, the encoding will be auto-detected and decoded for you.
	"""
	logging.info(f"Fetching page {url}...")
	try:
		result = await http_fetch(url, encoding=encoding)
		logging.info(f">>> Page fetched: {url}")
		return result
	except Exception as e:
		if isinstance(e, pkgtools.fetch.FetchError):
			raise e
		else:
			msg = f"Couldn't get_page due to exception {e.__class__.__name__}"
			logging.error(url + ": " + msg)
			logging.exception(e)
			raise pkgtools.fetch.FetchError(url, msg)


async def get_url_from_redirect(url):
	"""
	This function will take a URL that redirects and grab what it redirects to. This is useful
	for /download URLs that redirect to a tarball 'foo-1.3.2.tar.xz' that you want to download,
	when you want to grab the '1.3.2' without downloading the file (yet).
	"""
	logging.info(f"Getting redirect URL from {url}...")
	async with httpx.AsyncClient() as client:
		try:
			resp = await client.get(url=url, follow_redirects=False)
			if resp.status_code == 302:
				return resp.headers["location"]
		except httpx.RequestError as e:
			raise pkgtools.fetch.FetchError(url, f"Couldn't get_url_from_redirect due to exception {repr(e)}")


async def get_response_headers(url):
	"""
	This function will take a URL and grab its response headers. This is useful for obtaining
	information about a URL without fetching its body.
	"""
	async with httpx.AsyncClient() as client:
		resp = await client.get(url=url, follow_redirects=True)
		return resp.headers


# vim: ts=4 sw=4 noet
