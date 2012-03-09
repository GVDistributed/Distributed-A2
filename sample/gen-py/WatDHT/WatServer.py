#!/usr/bin/env python

import sys
import time
sys.path.append('./gen-py')
sys.path.append('/u5/gguruganesh/cs454/Distributed-A2/lib/python2.6/site-packages/')

from WatDHT import Iface,Processor
from ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

users = []

class WDHTHandler(Iface):
	def get(self, key):
		"""
			Parameters:
		"""
		return "0"

	def put(self, key, val, duration):
		"""
		Parameters:
		 - key
		 - val
		 - duration
		"""
		pass

	def join(self, nid):
		"""
		Parameters:
		 - nid
		"""
		pass

	def ping(self, ):
		print("Got a ping")
		return "GURU"

	def maintain(self, id, nid):
		"""
		Parameters:
		 - id
		 - nid
		"""
		pass

	def migrate_kv(self, nid):
		"""
		Parameters:
		 - nid
		"""
		pass

	def gossip_neighbors(self, nid, neighbors):
		"""
		Parameters:
		 - nid
		 - neighbors
		"""
		pass

	def closest_node_cr(self, id):
		"""
		Parameters:
		 - id
		"""
		pass

	def closest_node_ccr(self, id):
		"""
		Parameters:
		 - id
		"""
		pass



handler = WDHTHandler()
processor = Processor(handler)
transport = TSocket.TServerSocket(port=9090)
tfactory = TTransport.TBufferedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()

#server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

# You could do one of these for a multithreaded server
server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
#server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)

print('Starting the server...')
server.serve()
print('done.')
