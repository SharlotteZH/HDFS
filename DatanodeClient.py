#!/usr/bin/env python
 
from HDFSCommons import *

import sys
sys.path.append(BASE_PATH+"/fusepy")
sys.path.append(BASE_PATH+"/gen-py")

from BlockManager import *

from datanode import Datanode
from datanode.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from datetime import datetime,timedelta

class DatanodeClientCaller:
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

class DatanodeClient(DatanodeClientCaller):
	def __init__(self, host, port=DATANODE_PORT, timeout=TIMEOUT):
		# Make socket
		self.socket = TSocket.TSocket(host, port)
		self.socket.setTimeout(timeout)
		# Buffering is critical. Raw sockets are very slow
		self.transport = TTransport.TBufferedTransport(self.socket)
		# Wrap in a protocol
		self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
		# Create a client to use the protocol encoder
		self.client = Datanode.Client(self.protocol)
		# Connect!
		self.transport.open()
	
	def close(self):
		self.transport.close()

if __name__ == "__main__":
	try:
		client = DatanodeClient('ubuntu', DATANODE_PORT)
		print "ping..."
		client.client.ping()
		
		block = client.client.getBlock("6a0253af0806495aa6b5b97ba384b32c")
		print block
		
		timeStart = datetime.now()
		client.close()
			
	except Thrift.TException, tx:
		print "Error: %s" % (tx.message)
