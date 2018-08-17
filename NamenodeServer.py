#!/usr/bin/env python
  
from HDFSCommons import *

import sys
sys.path.append(BASE_PATH+"/fusepy")
sys.path.append(BASE_PATH+"/gen-py")
 
from BlockManager import *
from DatanodeClient import *

from namenode import Namenode
from namenode.ttypes import *
from namenode.ttypes import NamenodeOSError
 
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from errno import ENOENT

import string
import socket
import os
import exceptions
import uuid
import math
import random

from time import *
from datetime import *

from threading import Thread
from threading import Lock

import resource
import fcntl

# Thread that manages HDFS: locks, garbage collection, and replication
class NamenodeThread(Thread):
	def __init__(self, namenode):
		Thread.__init__(self)
		self.namenode = namenode
	
	# Perform maintenance tasks
	def run(self):
		i = 0
		while True:
			sleep(5.0)
			i += 1
			self.checkLocks()
			#if i % 20 == 0:
			self.garbageCollection()
			self.checkReplication()
	
	# Check all the locks of the filesystem...
	def checkLocks(self):
		# Check every block in the filesystem
		for dir in os.walk(self.namenode.rootBlocks):
			for file in dir[2]:
				blockId = file
				md5, size, locations = self.namenode.readBlockDescriptor(self.namenode.getBlockPath(blockId))
				if md5 == "-" and size == -1 and len(locations) == 1:
					location = locations[0]
					# Ask for the lock
					md5 = None
					size = -1
					try:
						datanode = DatanodeClient(location)
						md5 = datanode.client.getLock(blockId)
						if md5 != None and md5 != "-":
							size = datanode.client.getBlockSize(blockId)
						if md5 == "nolock" and size>=0:
							print location, "does not have the lock", blockId, size
							md5 = datanode.client.getBlockMD5(blockId)
							if md5 == "-":
								print "TODO the host does not have the block..."
							else:
								print md5, size, "obtained!"
					except Exception, e:
						print "Failed getting lock...", location, blockId, e
						pass
					finally:
						try:
							datanode.close()
						except UnboundLocalError:
							pass
					if md5 != None and md5 != "-" and md5 != "nolock" and size >= 0:
						self.namenode.releaseLock(blockId, md5, size, location)
	
	# Collect garbage: blocks not required anymore
	def garbageCollection(self):
		# Get blocks in the filesystem
		blocks = {}
		for dir in os.walk(self.namenode.rootBlocks):
			for file in dir[2]:
				blockId = file
				blocks[blockId] = True
		# Get files in the filesystem
		for dir in os.walk(self.namenode.root):
			for file in dir[2]:
				if not os.path.islink(dir[0]+"/"+file):
					try:
						path = os.path.normpath(dir[0]+"/"+file)
						path = path[len(self.namenode.root)+1:]
						for blockId, md5, size, locations in self.namenode.readFileDescriptor(self.namenode.root+"/"+path):
							try:
								del blocks[blockId]
							except Exception, e:
								#print "Block", blockId, "for file", self.namenode.root+"/"+path, "not present.", e, locations
								pass
					except Exception, e:
						print e
						pass
		if len(blocks) > 0:
			# Delete blocks that are not needed anymore
			if False:
				if len(blocks) < 5:
					print string.join(blocks.keys(), ", "), "not required anymore... collecting garbage!"
				else:
					print len(blocks), "blocks not required anymore... collecting garbage!"
			for blockId in blocks:
				md5, size, locations = self.namenode.readBlockDescriptor(self.namenode.getBlockPath(blockId))
				# If not locked
				if md5 != "-":
					error = False
					for location in locations:
						if self.namenode.isAlive(location):
							try:
								datanode = DatanodeClient(location)
								datanode.client.deleteBlock(blockId)
								datanode.close()
							except Exception, e:
								print "Error deleting block at", location+":", e
								error = True
					if not error:
						os.remove(self.namenode.getBlockPath(blockId))
	
	# Checks if blocks are under-replicated
	def checkReplication(self):
		try:
			# Get blocks in the filesystem
			blocks = {}
			for dir in os.walk(self.namenode.rootBlocks):
				for file in dir[2]:
					blockId = file
					blocks[blockId] = True
			# TODO multithread this process
			replicas = 0
			for blockId in blocks:
				md5, size, locations = self.namenode.readBlockDescriptor(self.namenode.getBlockPath(blockId))
				if md5 != "-" and len(locations)>0:
					missingReplicas = REPLICATION_MIN-len(locations)
					if missingReplicas > 0:
						replicas += 1
						newLocations = self.namenode.selectBlockLocation(replicas=missingReplicas, exclude=locations)
						for dstLocation in newLocations:
							srcLocation = locations[random.randint(0, len(locations)-1)]
							# Replicate block
							try:
								# This may take long, wait for it
								datanode = DatanodeClient(srcLocation, timeout=100*1000)
								datanode.client.replicateBlock(blockId, dstLocation)
								datanode.close()
							except Exception, e:
								print "Error replicating", blockId, "from", srcLocation, "to", dstLocation,":", e
				# Avoid replicating like crazy
				if replicas > 10:
					break
		except Exception, e:
			print e

# Namenode server
class NamenodeHandler:
	def __init__(self):
		# Filesystem
		self.root = NAMENODE_ROOT+"/fs"
		if not os.path.exists(self.root):
			os.makedirs(self.root)
		# Blocks
		self.rootBlocks = NAMENODE_ROOT+"/blocks"
		if not os.path.exists(self.rootBlocks):
			os.makedirs(self.rootBlocks)
		# Datanodes
		self.datanodes = {}
		# Maintenance thread
		self.thread = NamenodeThread(self)
		self.thread.start()
		# Locks for reading files
		self.locks = {}
	
	def chmod(self, path, mode):
		#print "chmod", path, mode
		try:
			return os.chmod(os.path.normpath(self.root+"/"+path), mode)
		except exceptions.OSError, e:
			#print e
			raise NamenodeOSError(e.errno, e.strerror)
	
	def chown(self, path, uid, gid):
		#print "chown", path, uid, gid
		try:
			return os.chown(os.path.normpath(self.root+"/"+path), uid, gid)
		except exceptions.OSError, e:
			#print e
			raise NamenodeOSError(e.errno, e.strerror)

	def create(self, path, mode):
		#print "create", path, mode
		ret = os.open(os.path.normpath(self.root+"/"+path), os.O_WRONLY | os.O_CREAT, mode)
		os.close(ret)
		return ret

	def getattr(self, path):
		#print "getattr", path
		try:
			st = os.lstat(os.path.normpath(self.root+"/"+path))
			ret = dict((key, str(getattr(st, key))) for key in ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
			if os.path.islink(os.path.normpath(self.root+"/"+path)):
				pass
			# Get size if this is a file
			elif os.path.isfile(os.path.normpath(self.root+"/"+path)):
				ret['st_size'] = 0
				ret['st_blocks'] = 0
				fd = self.readFileDescriptor(self.root+"/"+path)
				for blockd in fd:
					if blockd[2] == -1:
						# Get size from lock master
						locations = blockd[3]
						if len(locations) > 0:
							try:
								datanode = DatanodeClient(locations[0])
								auxSize = datanode.client.getBlockSize(blockd[0])
							except Exception, e:
								auxSize = 0
								print e
							finally:
								try:
									datanode.close()
								except UnboundLocalError:
									pass
							ret['st_size'] += auxSize
							# Calculate blocks
							ret['st_blocks'] += int(math.ceil(auxSize/512.0))
					else:
						ret['st_size'] += blockd[2]
						ret['st_blocks'] += int(math.ceil(blockd[2]/512.0))
				ret['st_size'] = str(ret['st_size'])
				ret['st_blocks'] = str(ret['st_blocks'])
			#ret['st_blksize'] = str(4096)
			return ret
		except exceptions.OSError, e:
			#print e
			raise NamenodeOSError(e.errno, e.strerror)

	def mkdir(self, path, mode):
		#print "mkdir",path,mode
		return os.mkdir(os.path.normpath(self.root+"/"+path), mode)

	def readdir(self, path):
		#print "readdir",path
		return ['.', '..'] + os.listdir(os.path.normpath(self.root+"/"+path))

	def readlink(self, path):
		#print "readlink", path
		linkObj = os.readlink(os.path.normpath(self.root+"/"+path))
		#linkObj = linkObj.replace(self.root+"/", "")
		return linkObj

	def rename(self, old, new):
		#print "rename", old, new
		try:
			return os.rename(os.path.normpath(self.root+"/"+old), os.path.normpath(self.root+"/"+new))
		except exceptions.OSError, e:
			#print e
			raise NamenodeOSError(e.errno, e.strerror)

	def rmdir(self, path):
		#print "rmdir",path
		return os.rmdir(os.path.normpath(self.root+"/"+path))

	def symlink(self, source, link_name):
		#print "symlink", source, link_name
		return os.symlink(source, os.path.normpath(self.root+"/"+link_name))

	def link(self, source, link_name):
		#print "link", source, link_name
		return os.link(os.path.normpath(self.root+"/"+source), os.path.normpath(self.root+"/"+link_name))
	
	def truncate(self, path, length):
		if length == 0:
			# Read file descriptor
			self.writeFileDescriptor(os.path.normpath(self.root+"/"+path), [])
		else:
			fd = self.readFileDescriptor(os.path.normpath(self.root+"/"+path))
			if length%BLOCK_SIZE > 0:
				fd = fd[0:(length/BLOCK_SIZE)+1] # truncate
			else:
				fd = fd[0:length/BLOCK_SIZE] # truncate
			self.writeFileDescriptor(os.path.normpath(self.root+"/"+path), fd)
	
	def unlink(self, path):
		#print "unlink",path
		return os.unlink(os.path.normpath(self.root+"/"+path))

	def utimens(self, path, atime, mtime):
		#print "utimens",path,(atime, mtime)
		return os.utime(os.path.normpath(self.root+"/"+path), (atime, mtime))
	
	# Block management
	def getBlockPath(self, blockId):
		# Take the 2 first numbers of the id to classify
		if not os.path.exists(self.rootBlocks+"/"+blockId[0:2]):
			os.makedirs(self.rootBlocks+"/"+blockId[0:2])
		return self.rootBlocks+"/"+blockId[0:2]+"/"+blockId
		
	def getBlocks(self, path):
		#print "getBlocks", path
		ret = []
		fd = self.readFileDescriptor(os.path.normpath(self.root+"/"+path))
		for blockd in fd:
			ret.append(blockd[0])
		return ret
		
	def addBlock(self, host, path, index):
		#print "addBlock",path
		# Read file descriptor
		fd = self.readFileDescriptor(os.path.normpath(self.root+"/"+path))
		# Add intermediate blocks
		for i in range(len(fd), index):
			fd.append(("-", "-", -1, []))
		# Create new block
		blockId = uuid.uuid4().hex
		if index < len(fd):
			fd[index] = (blockId, "-", -1, [host])
		else:
			fd.append((blockId, "-", -1, [host]))
		
		# Write file descriptor back
		self.writeFileDescriptor(os.path.normpath(self.root+"/"+path), fd)
		
		return blockId
	
	def registerBlock(self, host, blockId, md5):
		checkmd5, size, locations = self.readBlockDescriptor(self.getBlockPath(blockId))
		# Check that the file is actually the same
		if checkmd5 == md5:
			locations.append(host)
			self.writeBlockDescriptor(self.getBlockPath(blockId), md5, size, locations)
			return True
		else:
			return False
	
	def getBlockLocation(self, path, blockId, index):
		#print "getBlockLocation",path,blockId,index
		# Read file descriptor
		fd = self.readFileDescriptor(os.path.normpath(self.root+"/"+path))
		if fd[index][0] == blockId:
			md5 = fd[index][1]
			locations = fd[index][3]
			return [md5] + locations
		else:
			raise NamenodeOSError(ENOENT, "Block not found "+str(blockId))
	
	def selectBlockLocation(self, replicas=0, exclude=[]):
		ret = []
		availableNodes = []
		# Select available datanodes
		for avail in self.datanodes.keys():
			if self.isAlive(avail):
				availableNodes.append(avail)
		# Exclude datanodes
		for ex in exclude:
			if ex in availableNodes:
				availableNodes.remove(ex)
		# Select random locations
		for i in range(0, replicas):
			if len(availableNodes) > 0:
				newLocation = availableNodes[random.randint(0, len(availableNodes)-1)]
				ret.append(newLocation)
				availableNodes.remove(newLocation)
		return ret
	
	# Gets the lock
	def getLock(self, host, path, blockId, index):
		#print "getLock",host,path,blockId,index
		# Read file descriptor
		fd = self.readFileDescriptor(os.path.normpath(self.root+"/"+path))
		# Create new block
		newBlockId = uuid.uuid4().hex
		fd[index] = (newBlockId, "-", -1, [host])
		# Write file descriptor back
		self.writeFileDescriptor(os.path.normpath(self.root+"/"+path), fd)
		
		return newBlockId
	
	# Release lock in the DFS
	def releaseLock(self, blockId, md5, size, host):
		#print "releaseLock",path,blockId,index,md5, size
		# Store information
		try:
			self.writeBlockDescriptor(self.getBlockPath(blockId), md5, size, [host])
		except Exception, e:
			print e
	
	# Copy on write: it just copies the descriptor not the content
	def copyOnWrite(self, old, new):
		print "Copy-on-Write", old, new
		fd = self.readFileDescriptor(os.path.normpath(self.root+"/"+old))
		self.writeFileDescriptor(os.path.normpath(self.root+"/"+new), fd)
	
	# List of blocks of a file
	# [blocks] -> blockId, md5, size, locations
	def readFileDescriptor(self, filename):
		#print "readFileDescriptor: "#, get_open_fds()
		ret = []
		self.acquireFileDescriptorLock(filename)
		try:
			with open(filename, 'r') as f:
				for line in f:
					# Read block information
					blockId = line.replace("\n", "")
					try:
						md5,size,locations = self.readBlockDescriptor(self.getBlockPath(blockId))
						ret.append((blockId, md5, size, locations))
					except Exception, e:
						print "Error reading block", blockId, "of file", filename,":", e
		finally:
			self.releaseFileDescriptorLock(filename)
		return ret
	
	def writeFileDescriptor(self, filename, fd):
		#print "writeFileDescriptor: "#, get_open_fds()
		self.acquireFileDescriptorLock(filename)
		try:
			with open(filename, 'w') as f:
				for blockId, md5, size, locations in fd:
					f.write(blockId+"\n")
					self.writeBlockDescriptor(self.getBlockPath(blockId), md5, size, locations)
		finally:
			self.releaseFileDescriptorLock(filename)

	def readBlockDescriptor(self, filename):
		#print "readBlockDescriptor: "#, get_open_fds()
		self.acquireFileDescriptorLock(filename)
		try:
			with open(filename, 'r') as f:
				try:
					#blockId = f.readline().replace("\n", "")
					md5 = f.readline().replace("\n", "")
					size = int(f.readline().replace("\n", ""))
					locations = []
					for line in f:
						locations.append(line.replace("\n", ""))
				except ValueError as e:
					print "Error reading block descriptor:", filename
					print "Error:", e
					print "Line:", line
					print ret
		finally:
			self.releaseFileDescriptorLock(filename)
		return md5, size, locations
	
	def writeBlockDescriptor(self, filename, md5, size, locations):
		#print "writeBlockDescriptor: "#, get_open_fds()
		self.acquireFileDescriptorLock(filename)
		try:
			with open(filename, 'w') as f:
				f.write(md5+"\n")
				f.write(str(size)+"\n")
				for location in locations:
					f.write(location+"\n")
		finally:
			self.releaseFileDescriptorLock(filename)
	
	def acquireFileDescriptorLock(self, lockId):
		if lockId not in self.locks:
			self.locks[lockId] = Lock()
		self.locks[lockId].acquire()
	
	def releaseFileDescriptorLock(self, lockId):
		self.locks[lockId].release()
	
	def ping(self, datanodeId):
		self.datanodes[datanodeId] = datetime.now()
		#for datanodeId in self.datanodes:
			#print datanodeId, self.isAlive(datanodeId)

	def isAlive(self, datanodeId):
		ret = False
		if datanodeId in self.datanodes:
			if self.datanodes[datanodeId] != None and (datetime.now() - self.datanodes[datanodeId]) < timedelta(seconds=HEARTBEAT_LOST):
				ret = True
		return ret

if __name__ == "__main__":
	handler = NamenodeHandler()
	processor = Namenode.Processor(handler)
	transport = TSocket.TServerSocket(port=NAMENODE_PORT)
	tfactory = TTransport.TBufferedTransportFactory()
	pfactory = TBinaryProtocol.TBinaryProtocolFactory()

	#server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
	server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)
	
	print "Starting Namenode server..."
	server.serve()
	print "done!"
