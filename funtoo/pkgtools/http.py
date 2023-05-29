#!/usr/bin/env python3

import dyne.org.funtoo.metatools.pkgtools as pkgtools
import httpx

from metatools.fastpull.spider import FetchRequest

"""
This sub implements lower-level HTTP fetching logic, such as actually grabbing the data, sending the
proper headers and authentication, etc.
"""


async def get_page(url, encoding=None, is_json=False):
	"""
	This function performs a simple HTTP fetch of a resource. The response is cached in memory,
	and a decoded Python string is returned with the result. FetchError is thrown for an error
	of any kind.

	Use ``encoding`` if the HTTP resource does not have proper encoding and you have to set
	a specific encoding. Normally, the encoding will be auto-detected and decoded for you.
	"""
	request = FetchRequest(url=url)
	pkgtools.fetch.set_basic_auth(request)
	# Leverage the spider for this fetch. This bypasses the FPOS, etc:
	headers, result = await pkgtools.model.spider.http_fetch(request, encoding=encoding, is_json=is_json)
	return result


async def get_url_from_redirect(url):
	return await pkgtools.model.spider.get_url_from_redirect(url)


async def get_response_headers(url):
	"""
	This function will take a URL and grab its response headers. This is useful for obtaining
	information about a URL without fetching its body.
	"""
	async with httpx.AsyncClient() as client:
		resp = await client.get(url=url, follow_redirects=True)
		return resp.headers


# vim: ts=4 sw=4 noet
