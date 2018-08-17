#!/usr/bin/env python

from HDFSCommons import *

import sys
sys.path.append(BASE_PATH+"/fusepy")
sys.path.append(BASE_PATH+"/gen-py")

import math
import os
import socket

from sys import argv, exit
from time import time

from datetime import *

from errno import ETIMEDOUT, ECONNREFUSED, ENOENT

from fuse import FUSE, FuseOSError, Operations

from BlockManager import *
from DatanodeServer import *
from DatanodeClient import *

from threading import Thread

from namenode import Namenode
from namenode.ttypes import *

from datanode import Datanode
from datanode.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class DatanodeThread(Thread):
	def __init__(self, datanode):
		Thread.__init__(self)
		self.datanode = datanode
		
		processor = Datanode.Processor(self.datanode)
		transport = TSocket.TServerSocket(port=DATANODE_PORT)
		tfactory = TTransport.TBufferedTransportFactory()
		pfactory = TBinaryProtocol.TBinaryProtocolFactory()
		
		#self.server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
		self.server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)
	
	def run(self):
		try:
			self.server.serve()
		except Exception, e:
			print "Error in the Datanode server"
			print e

class HDFS(Operations):
	# Creator
	def __init__(self, hostnamenode=NAMENODE_HOST, portnamenode=NAMENODE_PORT, path='.'):
		self.root = path
		
		# Own namenode
		self.hostname = socket.gethostname()
		
		# Client to namenode
		self.hostnamenode = hostnamenode
		self.portnamenode = portnamenode
		self.namenode = None
		
		# Start datanode
		print "Starting Datanode server...",self.hostname
		self.datanode = DatanodeHandler(hostnamenode)
		DatanodeThread(self.datanode).start()
	
	def getNamenodeClient(self):
		if self.namenode == None:
			self.namenodeclient = NamenodeClient(self.hostnamenode, self.portnamenode)
			self.namenode = self.namenodeclient.client
		return self.namenode
	
	def closeNamenodeClient(self):
		if self.namenode != None:
			self.namenodeclient.close()
			self.namenode = None
	# Destructor
	def __del__(self):
		self.closeNamenodeClient()
	
	# Datanode
	# Get the information of a file block (id, md5, and location)
	def getBlockLocation(self, path, iBlock):
		# Get list of blocks
		if path in self.datanode.cacheFileBlocks and (datetime.now()-self.datanode.cacheFileBlocks[path][0]) < timedelta(seconds=self.datanode.EXP_TIME):
			timestamp, blocks = self.datanode.cacheFileBlocks[path]
		else:
			blocks = self.getNamenodeClient().getBlocks(path)
			self.datanode.cacheFileBlocks[path] = (datetime.now(), blocks)
		# Get location of the file
		if iBlock >= len(blocks):
			raise IndexError("Block "+str(iBlock)+" of file \""+str(path)+"\" does not exist")
		blockId = blocks[iBlock]
		if blockId in self.datanode.cacheBlockLocation and (datetime.now()-self.datanode.cacheBlockLocation[blockId][0]) < timedelta(seconds=self.datanode.EXP_TIME):
			timestamp, md5, locations = self.datanode.cacheBlockLocation[blockId]
		else:
			locations = self.getNamenodeClient().getBlockLocation(path, blockId, iBlock)
			md5 = locations.pop(0) # Getting md5
			self.datanode.cacheBlockLocation[blockId] = (datetime.now(), md5, locations)
		return blockId, md5, locations

	# Get the list of blocks of a file ([blockId...])
	def getBlocks(self, path):
		ret = None
		# Get list of blocks
		if path in self.datanode.cacheFileBlocks and (datetime.now()-self.datanode.cacheFileBlocks[path][0]) < timedelta(seconds=self.datanode.EXP_TIME):
			timestamp, blocks = self.datanode.cacheFileBlocks[path]
			ret = blocks
		else:
			blocks = self.getNamenodeClient().getBlocks(path)
			self.datanode.cacheFileBlocks[path] = (datetime.now(), blocks)
			ret = blocks
		return ret
	
	# Get a block from a remote location and store it
	def getBlock(self, locations, blockId, md5, lock=False):
		#print "getBlock", locations, blockId, md5
		if not self.datanode.isBlockLocal(blockId, md5):
			block = None
			auxLocations = list(locations)
			while block == None and len(auxLocations)>0:
				location = auxLocations.pop()
				try:
					datanode = DatanodeClient(location)
					print "getting", blockId, "from", location
					block = datanode.client.getBlock(blockId)
				except Exception, e:
					print "Could not get block", blockId, "from", location
					print e
					print "error finished"
				finally:
					try:
						datanode.close()
					except UnboundLocalError:
						pass
			# Store locally and register the block
			if block != None:
				self.datanode.putBlock(blockId, block, False)
	
	# Gets lock from Namenode
	def getLock(self, locations, path, iBlock, blockId, md5):
		# Get lock
		#print "getLock", locations, path, iBlock, blockId, md5
		newBlockId = self.getNamenodeClient().getLock(self.hostname, path, blockId, iBlock)
		# Make other replicas invalid
		for location in locations:
			if location != self.hostname:
				client = DatanodeClient(location)
				client.client.invalidateFile(path)
				client.close()
		return newBlockId
	
	def isLock(self, blockId):
		return self.datanode.isLock(blockId)
		
	def setLock(self, blockId):
		return self.datanode.setLock(blockId)
	
	def cpBlock(self, blockId, newBlockId):
		self.datanode.cpBlock(blockId, newBlockId)
	
	def readBlock(self, locations, blockId, md5, offset, size):
		#print "readBlock", locations, blockId, md5, offset, size
		# If file is locked and not local
		if md5 == "-" and locations[0] != self.hostname:
			# Get it from the remote node
			client = DatanodeClient(locations[0])
			aux = client.client.readBlock(blockId, offset, size)
			client.close()
			return aux
		else:
			# Get the block if it is not local
			self.getBlock(locations, blockId, md5)
			# Read the block data from local datanode
			return self.datanode.readBlock(blockId, offset, size)
	
	def putBlock(self, location, blockId, block, lock=False):
		if location == self.hostname:
			self.datanode.putBlock(blockId, block, False)
		else:
			client = DatanodeClient(location)
			aux = client.client.putBlock(blockId, block, False)
			client.close()
			return aux
	
	# Filesystem
	def chmod(self, path, mode):
		#print "chmod", path, mode
		if path in self.datanode.cacheFileBlocks:
			del self.datanode.cacheFileBlocks[path]
		try:
			return self.getNamenodeClient().chmod(path, mode)
		except Exception, e:
			raise FuseOSError(e.what)
	
	def chown(self, path, uid, gid):
		#print "chown", path, uid, gid
		if path in self.datanode.cacheFileBlocks:
			del self.datanode.cacheFileBlocks[path]
		try:
			return self.getNamenodeClient().chown(path, uid, gid)
		except Exception, e:
			raise FuseOSError(e.what)

	def create(self, path, mode):
		#print "create", path, mode
		if path in self.datanode.cacheFileBlocks:
			del self.datanode.cacheFileBlocks[path]
		self.getNamenodeClient().create(path, mode)
		return 0

	def getattr(self, path, fh=None):
		#print "getattr", path
		try:
			ret = {}
			aux = self.getNamenodeClient().getattr(path)
			# Convert to right map format (integers and floats)
			for key in aux:
				if key.find("time") >= 0:
					ret[key] = float(aux[key])
				else:
					ret[key] = int(aux[key])
		except socket.timeout:
			raise FuseOSError(ETIMEDOUT)
		except TTransport.TTransportException:
			raise FuseOSError(ECONNREFUSED)
		except socket.error, e:
			self.closeNamenodeClient()
			raise FuseOSError(e.errno)
		except Exception, e:
			raise FuseOSError(e.what)
		return ret
	
	def mkdir(self, path, mode):
		#print "mkdir", path, mode
		return self.getNamenodeClient().mkdir(path, mode)

	def readdir(self, path, fh):
		#print "readdir", path
		try:
			#return ['.', '..'] + self.getNamenodeClient().readdir(path)
			return self.getNamenodeClient().readdir(path)
		except TTransport.TTransportException:
			raise FuseOSError(ECONNREFUSED)

	def readlink(self, path):
		#print "readlink", path
		aux = self.getNamenodeClient().readlink(path)
		return aux

	def rename(self, old, new):
		#print "rename", old, new
		if old in self.datanode.cacheFileBlocks:
			del self.datanode.cacheFileBlocks[old]
		if new in self.datanode.cacheFileBlocks:
			del self.datanode.cacheFileBlocks[new]
		try:
			#return self.getNamenodeClient().rename(old, self.root + new)
			return self.getNamenodeClient().rename(old, new)
		except Exception, e:
			#print e
			raise FuseOSError(e.what)

	def rmdir(self, path):
		#print "rmdir", path
		return self.getNamenodeClient().rmdir(path)

	def symlink(self, link_name, source):
		#print "symlink", source, link_name
		return self.getNamenodeClient().symlink(source, link_name)

	def link(self, link_name, source):
		#print "link", source, link_name
		return self.getNamenodeClient().link(source, link_name)
		
	def truncate(self, path, length, fh=None):
		#print "truncate", path, length
		# Remove caching information
		if path in self.datanode.cacheFileBlocks:
			del self.datanode.cacheFileBlocks[path]
		if length == 0:
			# Remove all the file
			self.getNamenodeClient().truncate(path, length)
		else:
			# Truncate the file
			truncateBlock = int(math.ceil((1.0*length)/BLOCK_SIZE))
			blocks = self.getBlocks(path)
			# Truncate to smaller number of blocks: remove blocks
			if truncateBlock < len(blocks):
				self.getNamenodeClient().truncate(path, length)
			# Remove caching information
			if path in self.datanode.cacheFileBlocks:
				del self.datanode.cacheFileBlocks[path]
			blocks = self.getBlocks(path)
			# Truncate last block
			if len(blocks)>0:
				# Truncate last block of the truncate
				blockId,md5,locations = self.getBlockLocation(path, len(blocks)-1)
				auxLength = BLOCK_SIZE
				if len(blocks) == truncateBlock:
					auxLength = length%BLOCK_SIZE
					if auxLength == 0:
						auxLength = BLOCK_SIZE
				if md5 == "-" and len(locations)>0 and locations[0]!=self.hostname:
					# Block is locked in another host
					c = DatanodeClient(locations[0])
					c.client.truncateBlock(blockId, auxLength)
					c.close()
				else:
					# Get block and lock
					if not self.isLock(blockId):
						# Getting lock, locations, path, iBlock, blockId, md5
						self.getBlock(locations, blockId, md5)
						#newBlockId = self.getLock(locations, path, iBlock, blockId, md5)
						newBlockId = self.getLock(locations, path, len(blocks)-1, blockId, md5)
						# Create new block
						self.cpBlock(blockId, newBlockId)
						blockId = newBlockId
						self.setLock(blockId)
						# Purge file info
						if path in self.datanode.cacheFileBlocks:
							del self.datanode.cacheFileBlocks[path]
					# Perform the truncation
					self.datanode.truncateBlock(blockId, auxLength)
			# Truncate to a higher number of blocks: create new blocks
			if truncateBlock > len(blocks):
				for iBlockCheck in range(len(blocks), truncateBlock):
					# Create a block and make it empty
					blockId = self.createBlock(path, iBlockCheck)
					# Check if it is the last block to truncate.
					# If the last block to truncate is equal to the size of the block: make a full block too.
					if iBlockCheck < truncateBlock-1 or length%BLOCK_SIZE==0:
						self.datanode.truncateBlock(blockId, BLOCK_SIZE)
					else:
						self.datanode.truncateBlock(blockId, length%BLOCK_SIZE)
	def unlink(self, path):
		#print "unlink", path
		return self.getNamenodeClient().unlink(path)

	def utimens(self, path, times):
		#print "utimens", path, times
		return self.getNamenodeClient().utimens(path, times[0], times[1])

	def read(self, path, size, offset, fh):
		#print "read", path, size, offset
		buf = ""
		# Getting blocks
		iniBlock = offset/BLOCK_SIZE
		finBlock = int(math.ceil(1.0*(offset + size)/BLOCK_SIZE))
		for iBlock in range(iniBlock, finBlock):
			inOffset = 0
			if iBlock == iniBlock:
				inOffset = offset % BLOCK_SIZE
			inSize = BLOCK_SIZE
			if iBlock == finBlock-1:
				inSize = (offset+size) % BLOCK_SIZE
			# Get block information and location
			try:
				blockId,md5,locations = self.getBlockLocation(path, iBlock)
				buf += self.readBlock(locations, blockId, md5, inOffset, inSize)
			except Exception, e:
				print "read failed", path, iBlock, e
		return buf
	
	def write(self, path, data, offset, fh):
		#print "write", path, offset, offset/(1024.0*1024.0), len(data)
		# Getting blocks
		iniBlock = offset/BLOCK_SIZE
		finBlock = int(math.ceil(1.0*(offset + len(data))/BLOCK_SIZE))
		for iBlock in range(iniBlock, finBlock):
			# Get block addresses to write data
			inOffset = 0
			if iBlock == iniBlock:
				inOffset = offset % BLOCK_SIZE
			inSize = BLOCK_SIZE
			if iBlock == finBlock-1:
				inSize = (offset+len(data)) % BLOCK_SIZE
			# Get buffer addresses
			iniWriteData = (iBlock-iniBlock) * BLOCK_SIZE
			finWriteData = (iBlock+1-iniBlock) * BLOCK_SIZE
			if finWriteData > len(data):
				finWriteData = len(data)
			# Get block information and location
			try:
				# Getting block location...
				blockId,md5,locations = self.getBlockLocation(path, iBlock)
				if md5 == "-" and len(locations)>0 and locations[0]!=self.hostname:
					# Block is locked in another host
					#print "block", blockId, "is in another host. write to", locations[0]
					c = DatanodeClient(locations[0])
					c.client.writeBlock(blockId, data, offset)
					c.close()
				else:
					# Get block and lock
					if not self.isLock(blockId):
						#print "getting lock for block", blockId
						# Getting lock, locations, path, iBlock, blockId, md5
						self.getBlock(locations, blockId, md5)
						newBlockId = self.getLock(locations, path, iBlock, blockId, md5)
						# Create new block
						self.cpBlock(blockId, newBlockId)
						blockId = newBlockId
						self.setLock(blockId)
						# Purge file info
						if path in self.datanode.cacheFileBlocks:
							del self.datanode.cacheFileBlocks[path]
					# Perform the write
					#print "write locally to block", blockId
					self.datanode.writeBlock(blockId, data[iniWriteData:finWriteData], inOffset)
			except IndexError, e:
				#print "Block", iBlock, "of file", path, "does not exist"
				# Block does not exist, create it
				blockId = self.createBlock(path, iBlock)
				#print "write into block",  path, iBlock, blockId, offset, "("+str(offset/(1024.0*1024.0))+")", inOffset, "("+str(inOffset/(1024.0*1024.0))+")", len(data)
				self.datanode.writeBlock(blockId, data[iniWriteData:finWriteData], inOffset)
				
				#print "CHECKING PREVIOUS BLOCKS SIZES..."
				# Check if the previous block are fine...
				for iBlockCheck in range(iBlock-1, -1, -1):
					blockId,md5,locations = self.getBlockLocation(path, iBlockCheck)
					#print "->", iBlockCheck, blockId
					# If block does not exist, create it empty
					if blockId == "-":
						#print "Create empty block"
						blockId = self.createBlock(path, iBlockCheck)
						# Make it empty
						#print "Filling it with zeroes..."
						self.datanode.truncateBlock(blockId, BLOCK_SIZE)
					else:
						if md5 == "-" and len(locations)>0 and locations[0]!=self.hostname:
							# Host in another host
							size = DatanodeClient(locations[0]).client.getBlockSize(blockId)
							if size < BLOCK_SIZE:
								DatanodeClient(locations[0]).client.truncateBlock(blockId, BLOCK_SIZE)
							else:
								break
						else:
							# Local block
					
							size = self.datanode.getBlockSize(blockId)
							if size < BLOCK_SIZE:
								self.datanode.truncateBlock(blockId, BLOCK_SIZE)
							else:
								break
			except Exception, e2:
				print "[TODO] fix write:", e2
		return len(data)

	def createBlock(self, path, iBlock):
		# Get id for the new block
		blockId = self.getNamenodeClient().addBlock(self.hostname, path, iBlock)
		#print "add block", self.hostname, path, iBlock, "=>", blockId
		# Purge file info
		if path in self.datanode.cacheFileBlocks:
			del self.datanode.cacheFileBlocks[path]
		# Create new block and write data in
		self.datanode.createBlock(blockId)
		return blockId
	
if __name__ == "__main__":
	if len(argv) != 3:
		print 'usage: %s <host> <mountpoint>' % argv[0]
		exit(1)
	fuse = FUSE(HDFS(hostnamenode=argv[1], path=argv[2]), argv[2], foreground=True, nothreads=True, debug=False, allow_other=True)
