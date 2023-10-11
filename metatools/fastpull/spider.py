import asyncio
import hashlib
import logging
import os
import random
import ssl
import string
import threading
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from json import JSONDecodeError
from typing import Tuple, Dict
from urllib.parse import urlparse

import httpx
import rich.progress

log = logging.getLogger('metatools.autogen')


class FetchRequest:

	def __init__(self, url, retry=True, extra_headers=None, mirror_urls=None, username=None, password=None,
	             expected_hashes=None, final_name=None):
		assert url is not None
		self.url = url
		self.retry = retry
		self.extra_headers = extra_headers if extra_headers else {}
		self.mirror_urls = mirror_urls if mirror_urls else []
		# for basic auth
		self.username = username
		self.password = password
		# TODO: this was a last-minute add to FetchRequest and we could possibly leverage this in the BLOS.
		self.expected_hashes = expected_hashes if expected_hashes is not None else {}
		self.final_name = final_name

	@property
	def hostname(self):
		parsed_url = urlparse(self.url)
		return parsed_url.hostname

	@property
	def filename(self):
		if self.final_name:
			return self.final_name
		else:
			parsed_url = urlparse(self.url)
			return parsed_url.path.split("/")[-1]

	def set_auth(self, username=None, password=None):
		self.username = username
		self.password = password

	def __repr__(self):
		return f"metatools.fastpull.spider.FetchRequest<{self.url}>"


class FetchResponse:
	"""
    FetchResponse will be returned in the case of a successful download of a file. ``fetch_request``, the
    only argument, references the original request associated with this response.

    When a fetch has completed successfully, a FetchResponse will be in the following state: ``temp_path`` will
    point to the location where the file has been downloaded. ``final_data`` will be populated to contain
    calculated hashes and size for the downloaded file. ``completion_result`` will be set to the ultimate
    value of the completion pipeline, if any list of functions was provided for ``completion_pipeline``
    in the associated ``FetchRequest`` (this is how we add some post-processing, such as storing the
    result to the BLOS, to all successful downloads.)
    """

	temp_path = None

	def __init__(self, request: FetchRequest):
		self.request = request
		self.completion_result = None


class Download:
	"""
    ``Download`` represents an in-progress download, and has a mechanism for recording and notifying those waiting
    for this particular download to complete. It allows the definition of a "completion pipeline", which is a list
    of functions to call. The first function will get the result of the download as an argument.
    """

	def __init__(self, spider, request: FetchRequest, hashes=None, completion_pipeline=None):
		self.spider = spider
		self.request = request
		self.waiters = []
		self.completion_pipeline = [] if completion_pipeline is None else completion_pipeline
		self.hashes = hashes
		self.final_data = None
		self._temp_path = None
		self.filesize = None
		self.total = None
		self.fd = None
		self.hash_calc_dict = None
		self.download_task = None
		self.start_time = None

	def get_download_future(self):
		log.debug(f"Download.await_existing:{threading.get_ident()} {self.request.url}")
		fut = asyncio.get_running_loop().create_future()
		self.waiters.append(fut)
		return fut

	def notify_waiters(self, result: FetchResponse):
		log.debug(f"Download.notify_waiters:{threading.get_ident()} for {self.request.url}: result is {result}")
		for future in self.waiters:
			future.set_result(result)

	def throw_exception(self, ex):
		"""
        When a download blows up, we want to have all the waiters receive an exception too -- before we throw it ourselves.
        """
		for future in self.waiters:
			future.set_exception(ex)
		raise ex

	@property
	def temp_path(self):
		if self._temp_path is None:
			# Use MD5 to create the path for the temporary file to avoid collisions, but also add a random string to allow
			# multiple downloads of the same file from different instances or autogens to avoid collision.
			rand_str = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
			temp_name = hashlib.md5(self.request.url.encode('utf-8')).hexdigest()
			self._temp_path = os.path.join(self.spider.temp_path, f"{rand_str}-{temp_name}")
		return self._temp_path

	async def _http_fetch_stream(self, on_chunk):
		"""
		This is a low-level streaming HTTP fetcher that will call on_chunk(bytes) for each chunk. On_chunk is called with
		literal bytes from the response body so no decoding is performed. The final_data attribute still needs
		to be filled out (this is done by self.launch(), which calls this.)

		Also note that this method will now raise FetchError if it truly fails, though it also will capture some error
		conditions internally do to proper robustifying of downloads and handle common download failure conditions itself.
		"""
		client = await self.spider.acquire_http_client(self.request)
		headers, auth = self.spider.get_headers_and_auth(self.request)

		attempts = 0
		if self.request.retry:
			max_attempts = 3
		else:
			max_attempts = 1
		completed = False
		received_data = False
		resume = False
		while not completed and (resume or attempts < max_attempts):
			try:
				if not resume:
					self.reset()
				else:
					headers["Range"] = f"bytes{self.filesize}-"
					resume = False
				async with client.stream("GET", url=self.request.url, headers=headers, auth=auth, follow_redirects=True) as response:
					# We do not want to do 304. This should prevent it....
					for bad_key in ["If-None-Match", "If-Modified-Since"]:
						assert bad_key not in client.headers
					# TODO: add back partial resume support
					if response.status_code not in [200, 206]:
						if response.status_code in [400, 404, 410]:
							# These are legitimate responses that indicate that the file does not exist. Therefore, we
							# should not retry, as we should expect to get the same result.
							retry = False
						else:
							retry = True
						raise FetchError(self.request,
						                 f"HTTP fetch_stream Error {response.status_code}: {response.reason_phrase[:120]}",
						                 retry=retry)
					if "Content-Length" in response.headers:
						self.total = int(response.headers["Content-Length"])
					else:
						self.total = 0
					# download_task can legitimately be zero, so check explicitly against None (our "null"):
					if self.download_task is None:
						# Only start download progress display if the download takes a minimum # of seconds...
						filename = self.request.filename
						if self.total == 0:
							filename = f"(stream) {filename}"
						self.download_task = self.spider.progress.add_task("Download", filename=filename, total=self.total)
						log.debug(f"Added download task {self.download_task}, total {self.total}")
					# DO NOT USE aiter_raw(), below!! It will result in invalid downloads from some sites!
					async for chunk in response.aiter_bytes():
						bytes_received = on_chunk(chunk, response)
						if bytes_received:
							received_data = True
					if self.total and self.total != self.filesize:
						raise FetchError(f"Number of bytes received ({self.filesize}) does not match Content Length ({self.total})")
					completed = True
			except httpx.RequestError as e:
				if received_data:
					resume = True
					log.error(f"Download failure for {self.request.url}: {str(e)} -- attempting to resume")
					continue
				else:
					log.error(f"Download failure for {self.request.url}: {str(e)}")
				if attempts + 1 < max_attempts:
					attempts += 1
					log.warning(f"Retrying after download failure... {str(e)}")
					continue
				else:
					break
			finally:
				if self.download_task is not None:
					self.spider.progress.remove_task(self.download_task)
					self.download_task = None

		if not completed:
			raise FetchError(self.request, "http_fetch_stream failure")

	def reset(self):
		"""
		Reset all necessary things after an aborted download that we will retry. We have to start from the beginning.
		:return:
		"""
		os.makedirs(os.path.dirname(self.temp_path), exist_ok=True)
		if self.fd:
			self.fd.close()
		self.fd = open(self.temp_path, "wb")
		self.hash_calc_dict = {}
		self.filesize = 0
		self.total = None
		self.start_time = datetime.utcnow()
		for h in self.hashes:
			self.hash_calc_dict[h] = getattr(hashlib, h)()

	def on_chunk(self, chunk, response):
		got_bytes = len(chunk)
		if not got_bytes:
			return 0
		self.fd.write(chunk)
		for hash in self.hashes:
			self.hash_calc_dict[hash].update(chunk)
		self.filesize += got_bytes
		if self.download_task is not None:
			if self.total:
				self.spider.progress.update(self.download_task, completed=response.num_bytes_downloaded)
			else:
				self.spider.progress.update(self.download_task, completed=response.num_bytes_downloaded, total=response.num_bytes_downloaded)
		return got_bytes

	async def launch(self) -> None:
		"""
		This is the lower-level download method that wraps the _http_fetch_stream() call, and ensures hashes are generated.

		Upon successful completion of the download, this function will set self.final_data to the final_data (hashes and
		size) of the downloaded file. It will also execute the completion pipeline, if any. It will return None if you
		have no completion pipeline, and will otherwise return the result of the execution of the pipeline.

		This method throw a FetchError if it encounters some kind of fetching problem, though _http_fetch_stream()
		will try to recover from many (and will catch some FetchErrors internally to do this.)
		"""

		log.debug(f"WebSpider.launch:{threading.get_ident()} spidering {self.request.url}...")
		if not self.spider.rich:
			log.info(f"Spidering {self.request.url}")

		try:
			await self._http_fetch_stream(self.on_chunk)
		except FetchError as fe:
			raise fe
		finally:
			self.fd.close()

		final_data = {}
		for h in self.hashes:
			final_data[h] = self.hash_calc_dict[h].hexdigest()
		final_data['size'] = self.filesize
		self.final_data = final_data

		if self.completion_pipeline:
			# start by handing this Download object to the start of the pipeline:
			completion_result = self
			for completion_fn in self.completion_pipeline:
				log.debug(f"Calling completion function {completion_fn} with argument {completion_result}")
				completion_result = await completion_fn(completion_result)
			self.notify_waiters(completion_result)
			# TODO: we may have a race condition here, or an unhandled case for aborted download (traceback seen in output)
			#       File "/home/drobbins/development/funtoo-metatools/metatools/fastpull/core.py", line 75, in get_file_by_url
			#         blos_obj = await self.parent.spider.download(request, completion_pipeline=[self.parent.fetch_completion_callback])
			#       File "/home/drobbins/development/funtoo-metatools/metatools/fastpull/spider.py", line 322, in download
			#         response = await download.launch()
			#       File "/home/drobbins/development/funtoo-metatools/metatools/fastpull/spider.py", line 217, in launch
			#         completion_result = completion_fn(completion_result)
			#       File "/home/drobbins/development/funtoo-metatools/metatools/fastpull/core.py", line 177, in fetch_completion_callback
			#         blos_object = self.blos.insert_object(download.temp_path)
			#       File "/home/drobbins/development/funtoo-metatools/metatools/fastpull/blos.py", line 378, in insert_object
			#         new_hashes = calc_hashes(temp_path, missing)
			#       File "/home/drobbins/development/funtoo-metatools/metatools/hashutils.py", line 10, in calc_hashes
			#         with open(fn, "rb") as myf:
			#       FileNotFoundError: [Errno 2] No such file or directory: '/home/drobbins/repo_tmp/tmp/spider/8dafbe2c08150bfbeeec719150b5ae1d'
			return completion_result
		else:
			return None


class FetchError(Exception):
	"""
	When this exception is raised, we can set retry to True if the failure is something that could conceivably be
	retried, such as a network failure. However, if we are reading from a cache, then it's just going to fail again,
	and thus retry should have the default value of False.

	This exception should be raised for *all* fetch-related errors in metatools. The ``retry`` field is used internally
	to determine whether this is a request we should legitimately retry (like intermittent network issues) or if this
	was a hard-fail, and it's unlikely that retrying the operation is not likely to yield any benefit.
	"""

	def __init__(self, request: FetchRequest, msg, retry=False):
		self.request = request
		self.msg = msg
		self.retry = retry

	def __repr__(self):
		return f"{self.request.url}: {self.msg}"


class ContentNotModified(Exception):
	pass


class WebSpider:
	"""
	This class implements a Web Spider, which is used to quickly download a lot of things. This spider takes care
	of downloading the files, and will also calculate cryptographic hashes for what it downloads. This is because
	it's more efficient to calculate hashes while the download is being streamed rather than doing it after the
	file has been completely downloaded.

	Locking Code
	============

	The locking code below deserves some explanation. DL_ACTIVE tracks all the active downloads for
	*all* threads that are running. DL_ACTIVE_LOCK is a lock we use to access this dictionary, when
	we want to read or modify it.

	DOWNLOAD_SLOT is the mechanism we used to ensure we only have a certain number (specified by the
	value= parameter) of downloads active at once. Each active download will acquire a slot. When all
	slots are exhausted, any pending downloads will wait for an active slot before they can begin.
	"""

	DL_ACTIVE_LOCK = threading.Lock()
	DL_ACTIVE = dict()
	DOWNLOAD_SLOT = threading.Semaphore(value=20)
	FETCH_SLOT = defaultdict(lambda: threading.Semaphore(value=20))
	fetch_headers = {"User-Agent": "funtoo-metatools (support@funtoo.org)"}
	status_logger_task = None
	keep_running = True
	thread_ctx = threading.local()
	transport = None
	started = None
	limits = httpx.Limits(keepalive_expiry=30, max_keepalive_connections=100, max_connections=100)

	def __init__(self, temp_path, hashes):
		self.fetch_count = 0
		self.temp_path = temp_path
		self.hashes = hashes - {'size'}
		self.rich = True
		self.progress = rich.progress.Progress(
			"[progress.percentage]{task.percentage:>3.0f}%",
			rich.progress.TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
			rich.progress.BarColumn(bar_width=None),
			rich.progress.DownloadColumn(),
			rich.progress.TransferSpeedColumn(),
			transient=True
		)

	@property
	def http_clients(self):
		http_clients = getattr(self.thread_ctx, "http_clients", None)
		if http_clients is None:
			http_clients = self.thread_ctx.http_clients = {}
		return http_clients

	async def status_logger(self):
		while self.keep_running:
			await asyncio.sleep(0.1)
			dl_count = len(self.DL_ACTIVE)
			if not self.rich:
				if dl_count > 1:
					log.info(f"Spider active downloads: {len(self.DL_ACTIVE)}")
				elif dl_count == 1:
					log.info(f"Spider active download: {list(self.DL_ACTIVE.keys())[0]}")
		self.progress.stop()
		log.info("Status logger done.")

	async def start_asyncio_tasks(self):
		self.status_logger_task = asyncio.Task(self.status_logger())

	async def get_url_from_redirect(self, url):
		"""
		This function will take a URL that redirects and grab what it redirects to. This is useful
		for /download URLs that redirect to a tarball 'foo-1.3.2.tar.xz' that you want to download,
		when you want to grab the '1.3.2' without downloading the file (yet).
		"""
		request = FetchRequest(url=url)
		logging.info(f"Getting redirect URL from {url}...")
		client = await self.acquire_http_client(request)

		try:
			resp = await client.get(url=url, follow_redirects=False)
			if resp.status_code == 302:
				return resp.headers["location"]
		except httpx.RequestError as e:
			raise FetchError(url, f"Couldn't get_url_from_redirect due to exception {repr(e)}")

	async def start(self):
		if self.started:
			return
		# This turns on periodic logging of active downloads (to get rid of 'dots')
		self.progress.start()
		await self.start_asyncio_tasks()
		self.transport = httpx.AsyncHTTPTransport(retries=3, limits=self.limits)

	async def stop(self):
		if not self.started:
			return
		self.keep_running = False
		self.status_logger_task.cancel()
		try:
			await self.status_logger_task
		except:
			# This works around what is, from what I can tell, an asyncio bug. This, along with os._exit() in
			# doit, prevents a traceback that looks like this on exit with python3.7
			#
			#   File "/usr/lib/python3.7/asyncio/selector_events.py", line 271, in _remove_reader
			#     key = self._selector.get_key(fd)
			#   File "/usr/lib/python3.7/selectors.py", line 190, in get_key
			#     return mapping[fileobj]
			#   File "/usr/lib/python3.7/selectors.py", line 71, in __getitem__
			#     fd = self._selector._fileobj_lookup(fileobj)
			#   File "/usr/lib/python3.7/selectors.py", line 225, in _fileobj_lookup
			#     return _fileobj_to_fd(fileobj)
			#   File "/usr/lib/python3.7/selectors.py", line 42, in _fileobj_to_fd
			#     raise ValueError("Invalid file descriptor: {}".format(fd))
			# ValueError: Invalid file descriptor: -1
			pass

	async def download(self, request: FetchRequest, completion_pipeline=None):
		if not self.started:
			await self.start()

		"""
		This method attempts to start a download. It is what users of the spider should call, and will take into
		account any in-flight downloads for the same resource, which is most efficient and safe and will prevent
		multiple requests for the same file.
		
		The return value of this function varies depending on the value of ``completion_pipeline``. If
		``completion_pipeline`` is unset, the return value of this function will be ``None``.
		
		If a ``completion_pipeline`` of functions is specified, each item in the pipeline will be called in order.
		The first item in the pipeline will be given the ``Download`` (this object) as an argument. The second function in the
		pipeline will receive the output of the result of the first item, etc. The ultimate result of the pipeline
		will be returned to the caller. This allows the completion pipeline to perform actions and potentially throw
		exceptions, and return an object of interest to the caller of this function.
		"""

		if completion_pipeline is None:
			completion_pipeline = []
		download: Download = self.get_existing_download(request)
		if download:
			log.debug(f"Webspider.download:{threading.get_ident()} waiting on existing download for {request.url}")
			fut = download.get_download_future()
			result = await fut
			log.debug(
				f"Webspider.download:{threading.get_ident()} existing download for {request.url} completed, got {fut} {result}")
			return result
		else:
			log.debug(f"Webspider.download:{threading.get_ident()} starting new download for {request.url}")
			download = Download(self, request, hashes=self.hashes, completion_pipeline=completion_pipeline)
			async with self.acquire_download_slot():
				async with self.start_download(download):
					try:
						# This will actually fire off the download, and also handle calling the completion pipeline,
						# and if specified we will get the result of this completion pipeline as a return value:
						response = await download.launch()
						return response
					except FetchError as fe:
						# This set the exception for any waiters, too, before throwing in the towel ourselves:
						download.throw_exception(fe)

	def cleanup(self, response: FetchResponse):
		"""
		This is a utility function to clean up a temporary file provided by the spider, once the caller is done
		with it. Typically you would call this as the very end of a completion pipeline for a successful request.
		"""

		if os.path.exists(response.temp_path):
			try:
				os.unlink(response.temp_path)
			except FileNotFoundError:
				# FL-8301: address possible race condition
				pass

	async def acquire_http_client(self, request):
		# log.debug(f"acquire_http_client: count: {len(self.http_clients)} (request for {request.hostname}) count: {self.fetch_count}")
		if request.hostname not in self.http_clients:
			headers, auth = self.get_headers_and_auth(request)
			client = self.http_clients[request.hostname] = httpx.AsyncClient(transport=self.transport, http2=True,
			                                                                 base_url=request.hostname, headers=headers,
			                                                                 auth=auth, follow_redirects=True,
			                                                                 timeout=8)
			# httpx seems to cache these, which is bad. We don't want these from a previous client:
			for strip_header in ["If-None-Match", "If-Modified-Since"]:
				if strip_header in client.headers:
					del client.headers[strip_header]
			return client
		else:
			return self.http_clients[request.hostname]

	def get_headers_and_auth(self, request):
		if request.extra_headers:
			headers = self.fetch_headers.copy()
			headers.update(request.extra_headers)
		else:
			headers = self.fetch_headers
		if request.username and request.password:
			auth = (request.username, request.password)
		else:
			auth = None
		return headers, auth

	async def http_fetch(self, request: FetchRequest, is_json=False, encoding=None, extra_headers=None, allow_304=False) -> Tuple[
		Dict, str]:
		"""
UBER-NOTE:

This is a non-streaming HTTP fetcher that will properly convert the request to a Python string and return the entire
content as a string.

Use ``encoding`` if the HTTP resource does not have proper encoding and you have to set a specific encoding for string
conversion. Normally, the encoding will be auto-detected and decoded for you.

This method *will* return a FetchError if there was some kind of fetch failure, and this is used by the 'fetch cache'
so this is important.

NEW IMPLEMENTATION of basic HTTP resource fetch:

We always want to store the "Last-Modified" response header, which contains a date to compare against.
We then want to always send "If-Modified-Since" with this date, when requesting the resource again, and if
we get a 304 back, we should just use the cached resource. If we get a 200, we should update the resource.
This means that for every request, we should intentionally look in our fetch cache first, and see if we have
a resource with a "Last-Modified" header. And if we do, we use this in our request, and potentially we return
the entry from our fetch cache.

ETag should also be used, which will use an "If-None-Match: "etag"" request header and similarly return a 304.

In addition to this, metatools has its own built-in fetch_harness() which applies a level of caching, using
refresh_interval. Technically, this isn't a "cache" but just a default setting for how "fresh" we need something
to be for us to use it. By default, the refresh_interval is set to 15 minutes. (HOWEVER, IT LOOKS LIKE WE HAVE
A BUG WHERE THIS DEFAULTS to ZERO).

In addition to all this, we also have a 3-time-retry-the-fetch feature, which helps with flaky network and
intermittent Internet connectivity issues. Plus the ability to fall back to the cached resource if the fetch failed. In
this case, we are intentionally using a stale resource just for the sake of getting the autogen to work, and
we want this -- but we also currently don't really log these in a good way, and especially for production
tree regen, we would not see these unless we had a way to create a report that ran at the end of autogen.
I have a bug open to try to fix this.

New logic:"""
		async with self.acquire_fetch_slot(request):
			accept_304 = False
			http_client = await self.acquire_http_client(request)
			headers, auth = self.get_headers_and_auth(request)
			# TODO: add code to explicitly close all clients, above:
			try:
				# All 304-related headers should come in through extra_headers ONLY:
				if extra_headers:
					headers.update(extra_headers)

				for key_304 in ["If-None-Match", "If-Modified-Since"]:
					if key_304 in http_client.headers:
						# Safeguard: We don't want these stuck in the HTTP client's headers because they will apply to all requests
						del http_client.headers[key_304]
					if key_304 in headers:
						if allow_304:
							accept_304 = True
						else:
							del headers[key_304]

				response = await http_client.get(request.url, headers=headers, auth=auth, follow_redirects=True, timeout=15)
				log.debug(f'http_fetch: GET {response.status_code} {request.url}')
				if accept_304 and response.status_code == 304:
					raise ContentNotModified()
				if response.status_code != 200:
					if response.status_code in [400, 404, 410]:
						# No need to retry as the server has just told us that the resource does not exist.
						retry = False
					else:
						retry = True
					try:
						err_response = response.json()
					except JSONDecodeError:
						err_response = response.text
					log.error(
						f"Fetch failure for {request.url}: {response.status_code} {response.reason_phrase} {err_response}")
					if response.status_code == 304:
						log.error("We should not get status 304 (not modified) but we did.")
						log.error(f"  client request headers: {repr(http_client.headers)}")
						log.error(f"headers passed to client: {repr(headers)} with auth: {repr(auth)}")
						log.error(f"Extra headers:            {repr(extra_headers)}")
					raise FetchError(request,
					                 f"HTTP fetch Error: {request.url}: {response.status_code}: {response.reason_phrase} {err_response}",
					                 retry=retry)
				if is_json:
					try:
						return response.headers, response.json()
					except JSONDecodeError as jde:
						# TODO: report this via moonbeam
						raise FetchError(request, f"Error decoding JSON: {repr(jde)}", retry=False)
				if encoding:
					result = response.headers, response.content.decode(encoding)
				else:
					result = response.headers, response.text
				return result
			except (httpx.RequestError, ssl.SSLError) as re:
				# TODO: report this via moonbeam
				raise FetchError(request, f"Could not connect to {request.url}: {repr(re)}", retry=False)

	@asynccontextmanager
	async def acquire_download_slot(self):
		"""
		If you are inside this contextmanager, then it means you *have permission to start a download*.

		This code originally tried to do this, but it would deadlock::

			with DOWNLOAD_SLOT:
				yield

		This code ^^ will deadlock as hit the max semaphore value. The reason? When we hit the max value, it will block
		for a download slot in the current thread will FREEZE our thread's ioloop, which will prevent another asyncio
		task from executing which needs to *release* the download slot -- thus the deadlock.

		So instead of using this approach, we will attempt to acquire a download slot in a non-blocking fashion. If we
		succeed -- great. If not, we will asyncio loop to repeatedly attempt to acquire the slot with a slight delay
		between each attempt. This ensures that the ioloop can continue to function and release any download slots while
		we wait.
		"""
		try:
			while True:
				success = self.DOWNLOAD_SLOT.acquire(blocking=False)
				if not success:
					await asyncio.sleep(0.1)
					logging.info("WAITING ON SLOT")
					continue
				yield
				break
		finally:
			self.DOWNLOAD_SLOT.release()

	@asynccontextmanager
	async def acquire_fetch_slot(self, request):
		try:
			while True:
				success = self.FETCH_SLOT[request.hostname].acquire(blocking=False)
				if not success:
					await asyncio.sleep(0.1)
					logging.info("WAITING ON SLOT")
					continue
				else:
					self.fetch_count += 1
				yield
				break
		finally:
			self.FETCH_SLOT[request.hostname].release()

	@asynccontextmanager
	async def start_download(self, download):
		"""
		If you are inside the contextmanager, it means that you are ready to *start a download for a specific resource*.

		Automatically record the download as being active, and remove from our list when complete.

		While waiting for DL_ACTIVE_LOCK will FREEZE the current thread's ioloop, this is OK because we immediately release
		the lock after inspecting/modifying the protected resource (DL_ACTIVE in this case.)
		"""
		try:
			with self.DL_ACTIVE_LOCK:
				self.DL_ACTIVE[download.request.url] = download
			yield
		finally:
			with self.DL_ACTIVE_LOCK:
				if download.request.url in self.DL_ACTIVE:
					del self.DL_ACTIVE[download.request.url]

	def get_existing_download(self, request: FetchRequest):
		"""
		Get a download object for the file we're interested in if one is already being downloaded.
		"""
		with self.DL_ACTIVE_LOCK:
			if request.url in self.DL_ACTIVE:
				log.warning(
					f"WebSpider.get_existing_download:{threading.get_ident()} found active download for {request.url}")

				return self.DL_ACTIVE[request.url]

			log.debug(f"WebSpider.get_existing_download:{threading.get_ident()} no active download for {request.url}")
			return None
