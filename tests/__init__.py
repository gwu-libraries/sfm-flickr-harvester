import logging
import unittest
import os
import socket

try:
    from test_config import *
except ImportError:
    FLICKR_KEY = os.environ.get("FLICKR_KEY")
    FLICKR_SECRET = os.environ.get("FLICKR_SECRET")

test_config_available = True if FLICKR_KEY and FLICKR_SECRET else False

mq_port_available = True
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    s.connect(("mq", 5672))
except socket.error:
    mq_port_available = False

mq_username = os.environ.get("RABBITMQ_USER")
mq_password = os.environ.get("RABBITMQ_PASSWORD")
integration_env_available = mq_port_available and mq_username and mq_password


class TestCase(unittest.TestCase):
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("flickr_harvester").setLevel(logging.DEBUG)
    logging.getLogger("oauthlib").setLevel(logging.ERROR)
    logging.getLogger("requests_oauthlib").setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.ERROR)
    logging.getLogger("vcr").setLevel(logging.INFO)