#!/usr/bin/python3

import asyncio
import random
import string
import sys
from datetime import datetime

from zmq.asyncio import Context
from zmq.auth.asyncio import AsyncioAuthenticator

from metatools.zmq.key_monkey import *
from metatools.zmq.zmq_msg_breezyops import BreezyMessage, MessageType


class DealerConnection:

	in_flight_messages: dict = {}
	msg_id_counter = 0

	# endpoint="ipc:///tmp/feeds/"

	def __init__(self, app=None, endpoint="tcp://127.0.0.1:5556", identity=None):
		self.app = app
		self.endpoint = endpoint
		self.ctx = Context.instance()
		self.client = self._get_client()
		if identity is None:
			self.client.setsockopt(zmq.IDENTITY, (''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16)).encode("ascii")))
		else:
			self.client.setsockopt(zmq.IDENTITY, identity.encode('utf-8'))

	def _get_client(self):
		return self.ctx.socket(zmq.DEALER)

	def _connect(self):
		logging.info("Connecting to " + self.endpoint)
		self.client.connect(self.endpoint)

	async def setup(self):
		# EXAMPLE: asyncio.create_task(self.ping_task())
		pass

	async def start(self):
		await self.setup()

	def send_nowait(self, msg_obj: BreezyMessage):
		"""
		Send a message to RouterListener asynchronously, without waiting for a reply.

		:param msg_obj: The fully-formed BreezyMessage to send. We will send it as-is so all fields should
		  be set as needed.
		:return: None
		"""

		msg_obj.send(self)

	def async_send(self, msg_obj: BreezyMessage) -> asyncio.Future:
		"""
		The async_send() method is used to send a BreezyMessage to the RouterListener asynchronously and wait for
		a response. It can be used by client tools as well as by RequestHandlers that need to wait for a reply
		to satisfy an HTTP request, for example.

		:param msg_obj: The BreezyMessage to send.
		:return: a Future which can be awaited upon.

		It can be used as follows, although this is not ideal:

		bzm = BreezyMessage()
		bzm_resp = await hub_client.async_send(bzm)

		Be careful to only send messages that expect replies as this async implementation has no built-in
		concept of a timeout and your code could potentially block forever. Thus, a safer and much more
		recommended way to use async_send() is as follows:

		bzm = BreezyMessage()

		try:
			bzm_resp = await asyncio.wait_for(hub_client.async_send(bzm), timeout=10)
			# do other things here with the response.
		except asyncio.TimeoutError:
			# error!

		Also note that self.in_flight_messages is used to store all messages awaiting replies, and the
		current datetime that the message was sent is also stored. This allows for potential cleanup of
		un-replied messages for protocols that allow this.
		"""

		msg_obj.msg_id = str(self.msg_id_counter)
		self.msg_id_counter += 1

		fut = asyncio.Future()
		self.in_flight_messages[msg_obj.msg_id] = (fut, datetime.utcnow())
		msg_obj.send(self.client)
		return fut

	def ping(self):
		self.send_nowait(
			BreezyMessage(
				msg_type=MessageType.REQUEST,
				service="hub",
				action="ping",
				json_dict=self.register_args if self.register_args else {}
			)
		)

	async def ping_task(self):
		while True:
			await asyncio.sleep(45)
			self.ping()


class ZAPDealerConnection(DealerConnection):

	def __init__(self, app=None, keyname="client", remote_keyname="server", endpoint="tcp://127.0.0.1:5556", identity=None):
		self.keyname = keyname
		self.remote_keyname = remote_keyname
		self.keymonkey = KeyMonkey(keyname)

		super().__init__(app=app, endpoint=endpoint, identity=identity)

		self._connect()

	def _get_client(self):
		return self.keymonkey.setupClient(self.client, self.endpoint, self.remote_keyname)


class RouterListener:

	def __init__(self, app=None, bind_addr="tcp://127.0.0.1:5556"):

		self.app = app
		self.bind_addr = bind_addr
		self.ctx = Context.instance()
		self.identities = {}
		self.server = self.ctx.socket(zmq.ROUTER)
		self.server.bind(self.bind_addr)
		self.setup()

	def setup(self):
		pass

	async def start(self):
		while True:
			msg = await self.server.recv_multipart()
			self.on_recv(msg)

	def on_recv(self, msg):
		"""
		This is a stub method that should be expanded upon, which handles all incoming messages from clients.
		"""
		zmq_identity = msg[0]
		sys.stdout.write(f"HOWDY Received: {msg}\n")
		# Create a BreezyMessage object from the ZeroMQ multi-part message data:
		msg_obj = BreezyMessage.from_msg(msg[1:])
		# We would then do other things here with the object....


class ZAPRouterListener(RouterListener):

	zap_socket_count = 0

	def __init__(self, app=None, keyname="server", bind_addr="tcp://127.0.0.1:5556"):
		super().__init__(app=app, bind_addr=bind_addr)

		self.keyname = keyname
		self.bind_addr = bind_addr

		self.keymonkey = KeyMonkey(self.keyname)
		self.server = self.keymonkey.setupServer(self.server, self.bind_addr)

		self.auth = AsyncioAuthenticator(self.ctx)

		zap_socket_bind = "inproc://zeromq.zap.%2d" % ZAPRouterListener.zap_socket_count
		ZAPRouterListener.zap_socket_count += 1
		# Note: This requires a small patch in the funtoo variant of pyzmq. Unpatched pyzmq only allows one ZAP
		# socket per application. This allows an arbitrary number but we need to specify the bind socket:
		self.auth.start(zap_socket_bind)

		self.auth.allow("127.0.0.1")
		logging.info("ZAP enabled. Authorizing clients in %s." % self.keymonkey.authorized_clients_dir)
		if not os.path.isdir(self.keymonkey.authorized_clients_dir):
			raise FileNotFoundError(f"Directory not found: {self.keymonkey.authorized_clients_dir}")
		self.auth.configure_curve(domain='*', location=self.keymonkey.authorized_clients_dir)

# vim: ts=4 sw=4 noet
