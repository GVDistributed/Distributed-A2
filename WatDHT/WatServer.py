#!/usr/bin/env python

import sys
import time
import threading
import logging

from WatDHT import Iface,Processor
from ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from utils import periodic_thread
from utils import delayed_thread

from WatClient import WDHTClient
import Router

class WDHTHandler(Iface):

    def __init__(self, node):
        self.node = node
        self.router = Router.Router(self.node)

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
        logging.info("%032x is Joining", nid.int_id)

        next_node = self.router.closest_predecessor(nid)
        if next_node == self.node:
            nodes = self.router.get_nodes()
        else:
            nodes = WDHTClient(next_node.ip, next_node.port).join(nid)
        nodes.append(self.node)

        self.router.update([nid])
        self.router.debug()

        return nodes

    def ping(self):
        return self.node.id 

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
        Performs process of joining the system
        """
        client = WDHTClient(existing_host, existing_port)
        node_ids = client.join(self.node) 
        self.router.update(node_ids)
        self.router.debug()

    def maintain_neighbors(self):
        """
        Performs periodic neighbor maintenance by gossiping
        """
        logging.info("Maintaining Neighbors")

    def maintain_routing(self):
        """
        Performs periodic routing maintenance by pinging
        """
        logging.info("Maintaining Routing Table")

def start(handler, port):
    processor = Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    server.serve()


if __name__ == '__main__':
    
    logging.basicConfig(level=logging.DEBUG)#INFO)

    if not len(sys.argv) in (4, 6):
        print "Usage: ./server node_id ip port [existing_ip existing_port]"
        sys.exit(-1)

    print "Initializing ..."
    node_id = Router.hash(sys.argv[1])
    host = sys.argv[2]
    port = int(sys.argv[3])    
    node = NodeID(node_id, host, port)

    print "Node: %x (%s:%d)" % (node.int_id, host, port)
    handler = WDHTHandler(node)

    if len(sys.argv) == 6:
        existing_host = sys.argv[4]
        existing_port = int(sys.argv[5])

        print "Joining (%s:%d)" % (existing_host, existing_port)
        delayed_thread(lambda:handler.init(existing_host, existing_port), 1)

    # Starting maintenance threads
    periodic_thread(handler.maintain_neighbors, 10)
    periodic_thread(handler.maintain_routing, 30)

    # Start server
    print "Starting Server"
    start(handler, port)
    
    print "Done"
