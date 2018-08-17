#!/usr/bin/env python
 
from HDFSCommons import *

import sys
sys.path.append(BASE_PATH+"/fusepy")
sys.path.append(BASE_PATH+"/gen-py")

from BlockManager import *
from NamenodeClient import *
from DatanodeClient import *

from datanode import Datanode
from datanode.ttypes import *
 
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from time import *
from datetime import *

from threading import Thread
from threading import Lock

import socket

import os

# Thread that maintains HDFS topology by sending heartbeats
class DatanodeThread(Thread):
	def __init__(self, datanode):
		Thread.__init__(self)
		self.datanode = datanode
		self.connected = False
	
	def run(self):
		# Perform maintenance tasks
		while True:
			self.sendHeartBeat()
			sleep(HEARTBEAT_PERIOD)

	# Send heartbeat periodically
	def sendHeartBeat(self):
		try:
			# Send local information: hostname
			namenode = NamenodeClient(self.datanode.hostnamenode)
			namenode.client.ping(self.datanode.hostname)
			namenode.close()
			if not self.connected:
				print "Connected to Namenode!"
				self.connected = True
		except Exception, e:
			if self.connected:
				print "Lost contact with Namenode..."
				self.connected = False

# Datanode server
class DatanodeHandler:
	def __init__(self, hostnamenode):
		if not os.path.exists(DATANODE_ROOT):
			os.makedirs(DATANODE_ROOT)
		self.root = DATANODE_ROOT
		
		# Datanode own address
		self.hostname = socket.gethostname()
		# Namenode address
		self.hostnamenode = hostnamenode
		
		# Caches
		self.EXP_TIME = 5 # seconds
		self.cacheFileBlocks = {} # filename -> (time, list<blockId>)
		self.cacheBlockLocation = {} # blockId -> (time, md5, locations)
		self.locks = {}
		
		# Maintenance thread
		self.thread = DatanodeThread(self)
		self.thread.start()
	
	# Internal methods
	def getBlockPath(self, blockId):
		# Take the 2 first numbers of the id to classify
		if not os.path.exists(self.root+"/"+blockId[0:2]):
			os.makedirs(self.root+"/"+blockId[0:2])
		return self.root+"/"+blockId[0:2]+"/"+blockId
	
	# Create a new block
	def createBlock(self, blockId, lock=True):
		#print "create block", blockId
		# Write block
		with open(self.getBlockPath(blockId), 'w') as f:
			f.write("")
		# Write metadata
		with open(self.getBlockPath(blockId)+".info", 'w') as f:
			f.write("-")
		# Write lock
		if lock:
			with open(self.getBlockPath(blockId)+".lock", 'w') as f:
				f.write("-")
	
	# Read content from a local block
	def readBlock(self, blockId, offset, size):
		#print "read block", blockId, offset, size
		# Read data from disk
		with open(self.getBlockPath(blockId), 'r') as f:
			data = f.read()
		# Read metadata
		with open(self.getBlockPath(blockId)+".info", 'r') as f:
			md5 = f.read()
		if size > 0:
			return data[offset:offset+size]
		else:
			return data[offset:]

	def writeBlock(self, blockId, data, offset):
		with open(self.getBlockPath(blockId), 'r+') as f:
			f.seek(offset)
			f.write(data)
		self.locks[blockId] = datetime.now()
	
	def truncateBlock(self, blockId, size):
		#if self.getBlockSize(blockId) < size:
		with open(self.getBlockPath(blockId), 'r+') as f:
			f.truncate(size)
	
	def cpBlock(self, blockId, newBlockId):
		#print "cp block", blockId, newBlockId
		# Read data from disk
		with open(self.getBlockPath(blockId), 'r') as f:
			data = f.read()
		# Read metadata
		with open(self.getBlockPath(blockId)+".info", 'r') as f:
			md5 = f.read()
		# Write block to disk
		with open(self.getBlockPath(newBlockId), 'w') as f:
			f.write(data)
		# Write metadata
		with open(self.getBlockPath(newBlockId)+".info", 'w') as f:
			f.write(md5)
	
	# Check if block exists
	def isBlockLocal(self, blockId, md5):
		#print "is block local", blockId, md5
		ret = False
		if os.path.isfile(self.getBlockPath(blockId)):
			# Read metadata
			with open(self.getBlockPath(blockId)+".info", 'r') as f:
				md5check = f.read()
			if md5 == "-" or md5check == md5:
				ret = True
		return ret
	
	def setLock(self, blockId):
		#print "set lock"
		if not os.path.isfile(self.getBlockPath(blockId)+".lock"):
			with open(self.getBlockPath(blockId)+".lock", 'w') as f:
				f.write("-")
	
	def isLock(self, blockId):
		#print "isLock"
		ret = False
		if os.path.isfile(self.getBlockPath(blockId)+".lock"):
			ret = True
		return ret
	
	# Remote methods
	def ping(self):
		print "ping() from " + socket.gethostbyname(socket.gethostname())
	
	def getBlockSize(self, blockId):
		#print "getBlockSize"
		ret = 0
		try:
			st = os.lstat(os.path.normpath(self.getBlockPath(blockId)))
			ret = getattr(st, 'st_size')
		except Exception, e:
			print "Cannot get size for block", blockId, ":", e
		return ret
	
	def getBlockMD5(self, blockId):
		#print "getBlockSize"
		ret = "-"
		try:
			with open(self.getBlockPath(blockId)+".info", 'r') as f:
				ret = f.read()
		except Exception, e:
			print "Cannot get size for block", blockId, ":", e
		return ret
	
	# Get the md5 for the block
	def getLock(self, blockId):
		#print "getLock", blockId
		ret = "-"
		# Read data
		with open(self.getBlockPath(blockId), 'r') as f:
			data = f.read()
			block = BlockManager(data)
			ret = block.md5
		return ret
	
	# Get block from local filesystem
	def getBlock(self, blockId):
		#print "get block", blockId
		block = None
		# Check in disk
		if os.path.isfile(self.getBlockPath(blockId)):
			# Read data
			with open(self.getBlockPath(blockId), 'r') as f:
				data = f.read()
				block = BlockManager(data)
			# Read metadata
			with open(self.getBlockPath(blockId)+".info", 'r') as f:
				md5 = f.read()
			# Checking data integrity
			if block.md5 != md5:
				print "Error reading block", blockId, "md5 doesn't match:", block.md5, md5
				block = None
		return block
	
	# Write block in local filesystem
	def putBlock(self, blockId, block, lock=False):
		#print "put block", blockId
		# Write block
		with open(self.getBlockPath(blockId), 'w') as f:
			f.write(block.data)
		# Write metadata
		with open(self.getBlockPath(blockId)+".info", 'w') as f:
			f.write(block.md5)
		# Write lock
		if lock:
			with open(self.getBlockPath(blockId)+".lock", 'w') as f:
				f.write(block.md5)
		# Register it in the namenode
		self.registerBlock(blockId, block.md5)
	
	# Register a block in the Namenode
	def registerBlock(self, blockId, md5):
		if self.isBlockLocal(blockId, md5):
			namenode = NamenodeClient(self.hostnamenode)
			namenode.client.registerBlock(self.hostname, blockId, md5)
			namenode.close()
	
	# Replicate a block to another node
	def replicateBlock(self, blockId, dstLocation):
		#print "Replicate", blockId, "to", dstLocation
		try:
			block = self.getBlock(blockId)
			if block != None:
				datanode = DatanodeClient(dstLocation)
				datanode.client.putBlock(blockId, block, False)
				datanode.close()
		except Exception, e:
			print "Error putting", blockId, "to", dstLocation,":", e
	
	# Delete a block
	def deleteBlock(self, blockId):
		try:
			os.remove(self.getBlockPath(blockId))
		except Exception:
			pass
		try:
			os.remove(self.getBlockPath(blockId)+".info")
		except Exception:
			pass
		try:
			os.remove(self.getBlockPath(blockId)+".lock")
		except Exception:
			pass
	
	# Get the lock for the file: md5, size
	def getLock(self, blockId):
		#print "getLock", blockId
		ret = "-"
		if self.isLock(blockId):
			# Check if we can release the lock
			release = False
			if blockId not in self.locks or (datetime.now()-self.locks[blockId]) > timedelta(seconds=self.EXP_TIME):
				release = True
			if release:
				# Read data
				with open(self.getBlockPath(blockId), 'r') as f:
					data = f.read()
					block = BlockManager(data)
				ret = block.md5
				# Write metadata
				with open(self.getBlockPath(blockId)+".info", 'w') as f:
					f.write(block.md5)
				# Remove lock file
				if os.path.isfile(self.getBlockPath(blockId)+".lock"):
					os.remove(self.getBlockPath(blockId)+".lock")
		else:
			print "I don't have the lock for", blockId
			ret = "nolock"
		return ret
	
	def invalidateFile(self, path):
		#print "invalidate file", path
		ret = False
		if path in self.cacheFileBlocks:
			del self.cacheFileBlocks[path]
			ret = True
		return ret
