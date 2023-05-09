#!/usr/bin/python3

class MultiPartMessage(object):

	header = b"PING"

	"""In child classes, create an __init__ method that takes arguments containing message payload."""

	@classmethod
	def recv(cls, socket):
		"""Reads key-value message from socket, returns new instance."""
		return cls.from_msg(socket.recv_multipart())

	@property
	def msg(self):
		return [self.header]

	def send(self, socket, identity=None):
		"""Send message to socket"""
		msg = self.msg
		if identity:
			msg = [identity] + msg
		socket.send_multipart(msg)

	@classmethod
	def from_msg(cls, msg):
		"""Construct a MetricsMessage from a pyzmq message. This will need to be overridden in child classes."""
		if len(msg) != 1 or msg[0] != cls.header:
			# invalid
			return None
		return cls()

# vim: ts=4 sw=4 noet
