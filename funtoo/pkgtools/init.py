#!/usr/bin/env python3
import asyncio
import os
import threading
import yaml

hub = None


def load_autogen_config():
	path = os.path.expanduser("~/.autogen")
	if os.path.exists(path):
		with open(path, "r") as f:
			return yaml.safe_load(f)
	else:
		return {}


def __init__():
	hub.AUTOGEN_CONFIG = load_autogen_config()
