#!/usr/bin/env python

import sys
import time
import threading

from WatDHT import Iface,Processor
from ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

users = []

class WDHTHandler(Iface):

    def __init__(self, node):
        self.node = node

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

    def init(self, existing_host, existing_port):
        """
        Performs process of joining the DHT system
        """
        pass

def start(handler, port):
    processor = Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

if __name__ == '__main__':

    print "Initializing"

    # Parse and pack integer node id into binary string 
    node_id = int(sys.argv[1])
    node_id = NodeID.to_id(node_id)

    # Create our node object
    host = sys.argv[2]
    port = int(sys.argv[3])    
    node = NodeID(node_id, host, port)

    print "Node: %x (%s:%d)" % (node.int_id, host, port)

    handler = WDHTHandler(node)

    if len(sys.argv) == 6:
        existing_host = sys.argv[4]
        existing_port = int(sys.argv[5])

        print "Joining (%s:%d)" % (existing_host, existing_port)

        threading.Thread(target=lambda:handler.init(existing_host, existing_port)).start()

    # Start server
    print "Starting Server"
    start(handler, port)
    
    print "Done"
