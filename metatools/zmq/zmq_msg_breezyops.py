#!/usr/bin/python3

from metatools.zmq.zmq_msg_core import MultiPartMessage
from enum import Enum
import logging
from bson import ObjectId
from bson.json_util import loads, dumps, CANONICAL_JSON_OPTIONS


class MessageType(Enum):
	REQUEST = "req"
	RESPONSE = "resp"
	INFO = "info"


class BreezyMessage(MultiPartMessage):

	header = b"BRZ"

	def __init__(self, msg_id=None, device_key=None, msg_type=MessageType.INFO, service="", action="", json_dict=None):

		"""
		:param msg_id:     Message ID is an identifier that uniquely identifies a request/reply pairing. If a client
		                   sends a message and expects a reply, it will have the msg_id set to a value. The response's
		                   msg_id will have the same value, thus allowing the client to identify the received message
		                   as a response to the original message. This is necessary for asynchronous messages to work
		                   properly. The msg_id is intended to be set by the client and always be an integer value
		                   (typically increasing by one with each new client request.)
		:param device_key: A Device Key is a persistent key that is associated with a particular device. This is used
						   to uniquely identify mobile devices once they are registered. Registered devices are
						   associated with certain privileges based on the validation of the device/Device Key.
						   Device keys are an essential mechanism for connecting a mobile device to a particular
						   user account.
		:param msg_type:   One of the MessageType Enum to identify whether the message is a request, a response, or
						   just informational.
		:param service:    A string that uniquely identifies the service. Services advertise these names, and clients
						   specify services using the same string.
		:param action:     Look at this as the 'method name' of the service. Used in requests and responses.
		:param json_dict:  In the case of msg_type == MessageType.REQUEST, this data is passed to the service as
						   arguments. In the case of msg_type == MessageType.RESPONSE, this is the data being returned
						   to the caller. The contents are a JSON dictionary which can contain arbitrary data.
		"""

		self.msg_id = msg_id
		if type(device_key) == str:
			self.device_key = ObjectId(device_key)
		elif type(device_key) == ObjectId:
			self.device_key = device_key
		elif device_key is None:
			self.device_key = None
		else:
			raise TypeError("Unknown type for Device Key: %s" % type(device_key))
		self.msg_type = msg_type
		self.action = action
		self.service = service
		self.json_dict = json_dict if json_dict is not None else {}

	@property
	def msg(self):
		return [self.header,
						self.msg_id.encode("utf-8") if self.msg_id is not None else b"",
						str(self.device_key).encode("utf-8") if self.device_key is not None else b"",
						self.msg_type.value.encode("utf-8"),
						self.service.encode("utf-8"),
						self.action.encode("utf-8"),
						dumps(self.json_dict, json_options=CANONICAL_JSON_OPTIONS).encode("utf-8")
			]

	def log(self):
		pass

	def as_serializable(self):
		return {
			"header": "BRZ",
			"msg_id": self.msg_id,
			"device_key": str(self.device_key),
			"msg_type": self.msg_type.value,
			"service": self.service,
			"action": self.action,
			"json_dict": self.json_dict
		}

	def response(self, json_dict):
		return BreezyMessage(
			service=self.service,
			action=self.action,
			msg_id=self.msg_id,
			device_key=self.device_key,
			msg_type=MessageType.RESPONSE,
			json_dict=json_dict
		)

	@classmethod
	def from_msg(cls, msg):

		"""Construct a BreezyMessage from a pyzmq message"""

		if len(msg) != 7 or msg[0] != cls.header:
			logging.error("Invalid Message:", msg)
			return None
		return cls(msg[1].decode("utf-8"), # msg_id
						ObjectId(msg[2].decode("utf-8")) if msg[2] != b"" else None, # device_key
						MessageType(msg[3].decode("utf-8")) if msg[3] != b"" else None, # msg_type
						msg[4].decode("utf-8"), # service
						msg[5].decode("utf-8"), # action
						loads(msg[6].decode("utf-8"), json_options=CANONICAL_JSON_OPTIONS)
					)# json_dict

# vim: ts=4 sw=4 noet
