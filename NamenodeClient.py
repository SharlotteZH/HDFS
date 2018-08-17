#!/usr/bin/env python
 
from HDFSCommons import *

import sys
sys.path.append(BASE_PATH+"/fusepy")
sys.path.append(BASE_PATH+"/gen-py")

from sys import argv, exit

from namenode import Namenode
from namenode.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from datetime import datetime,timedelta

class NamenodeClientCaller:
    def __call__(self, op, path, *args):
        print '->', op, path, repr(args)
        ret = '[Unhandled Exception]'
        try:
            ret = getattr(self.client, op)(path, *args)
            return ret
        except OSError, e:
            ret = str(e)
            raise
        finally:
            print '<-', op, repr(ret)

class NamenodeClient(NamenodeClientCaller):
	def __init__(self, host, port=NAMENODE_PORT, timeout=TIMEOUT):
		# Make socket
		self.socket = TSocket.TSocket(host, port)
		self.socket.setTimeout(timeout)
		# Buffering is critical. Raw sockets are very slow
		self.transport = TTransport.TBufferedTransport(self.socket)
		# Wrap in a protocol
		self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
		# Create a client to use the protocol encoder
		self.client = Namenode.Client(self.protocol)
		# Connect!
		self.transport.open()
	
	def close(self):
		self.transport.close()

if __name__ == "__main__":
	if len(argv) != 4:
		print 'usage: %s <namenode> <path> <path2>' % argv[0]
		exit(1)
	try:
		print "Copying...", argv[2], argv[3]
		client = NamenodeClient(argv[1], NAMENODE_PORT)
		client.client.copyOnWrite(argv[2], argv[3])
		client.close()
	except Exception, e:
		print e
	

