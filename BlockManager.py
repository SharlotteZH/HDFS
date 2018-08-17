#!/usr/bin/env python

import sys
sys.path.append('./gen-py')

from datanode import Datanode
from datanode.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import hashlib

class BlockManager(Block):
	def __init__(self, data):
		md5data = hashlib.md5(data).hexdigest()
		self.md5 = md5data
		self.data = data
	
	@staticmethod
	def check(block):
		md5Check = md5.new(block.data).hexdigest()
		return md5Check == block.md5
