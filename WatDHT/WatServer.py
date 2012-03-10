#!/usr/bin/env python

import sys
import time
import threading
import logging

from WatDHT import Iface, Processor
from ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from utils import periodic_thread
from utils import delayed_thread
from utils import wait_on
from utils import readOnly
from utils import writeLock

from ReadWriteLock import ReadWriteLock
from WatClient import WDHTClient
import Router

class WDHTHandler(Iface):

    bootstrapped = threading.Event()
    migratelock = ReadWriteLock() 

    def __init__(self, node):
        self.node = node
        self.router = Router.Router(self.node)

    @wait_on(bootstrapped)
    @readOnly(migratelock)
    def get(self, key):
        """
            Parameters:
        """
        return "0"

    @wait_on(bootstrapped)
    @readOnly(migratelock)
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
        if next_node.id == self.node.id:
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
        cur = None
        closest = self.router.closest_predecessor(NodeId(id,-1,-1))
        if (closest is not self):
            client = WDHTClient(closest.ip, closest.port)
            cur = client.maintain(id,nid)
        else:
            cur = self.router.get_neighbors()
            cur.append(self.node.id)
        self.router.update([nid])
        return cur

    @writeLock(migratelock, 0, lambda: WatDHTException(WatDHTErrorType.OL_MIGRATION_IN_PROGRESS))
    def migrate_kv(self, id):
        successor = self.router.neighbor_set.get_successor()
        logging.info("Migrating to %032x", successor.int_id)
        if successor.id != id:
            raise WatDHTException(WatDHTErrorType.INCORRECT_MIGRATION_SOURCE, node=successor)

        # prepare and remove key-value dict from the store here

        return {}

    @writeLock(migratelock)
    def prv_migrate_from(self):
        # Migrate from predecessor
        backoff = 0.1
        while True:
            predecessor = self.router.neighbor_set.get_predecessor()
            logging.info("Migrating from %032x", predecessor.int_id)
            try:
                kvs = WDHTClient(predecessor.ip, predecessor.port).migrate_kv(self.node.id)
                break

            except WatDHTException, e:
                if e.error_code == WatDHTErrorType.INCORRECT_MIGRATION_SOURCE:
                    self.router.update([e.node])
                    logging.debug("Incorrect migration source")
                elif e.error_code == WatDHTErrorType.OL_MIGRATION_IN_PROGRESS:
                    backoff *= 1.1
                    time.sleep(backoff)
                    logging.debug("Backing off")
                else:
                    raise

        # Populate store with key values
        # ...

    def gossip_neighbors(self, nid, neighbors):
        """
        Parameters:
         - nid
         - neighbors
        """
        cur = self.router.neighbor_set.get_neighbors()
        cur.append(self.node.id)
        neighbors.append(nid)
        self.router.neighbor_set.update(neighbors)
        return cur

    def closest_node_cr(self, id):
        """
        Parameters:
         - id
        """
        cur = self.router.closest_successor(self,NodeID(id,-1,-1))
        if (cur.id == self.node.id):
            return cur
        else:
            client = WDHTClient(cur.ip, cur.port)
            return client.closest_node_cr(id)

    def closest_node_ccr(self, id):
        """
        Parameters:
         - id
        """
        cur = self.router.closest_predecessor(self,NodeID(id,-1,-1))
        if cur.id == self.node.id:
            return cur
        else:
            return WDHTClient(cur.ip, cur.port).closest_predecessor(id)

    def prv_init_origin(self):
        """
        Initiate the node given that it's the very first
        """
        self.bootstrapped.set()

    def prv_init(self, existing_host, existing_port):
        """
        Performs process of joining the system
        """

        # Find the predecessor and boostrap
        logging.info("Finding predecessor and bootstrapping")
        node_ids = WDHTClient(existing_host, existing_port).join(self.node) 
        self.router.update(node_ids)
        self.router.debug()

        # Enable receiving requests
        self.bootstrapped.set()

        self.prv_migrate_from()

        # Maintain routing table
        # ...

        logging.info("Initialized")
        
 
    def prv_maintain_neighbors(self):
        """
        Performs periodic neighbor maintenance by gossiping
        """
        def helper(L, t):
            """ 
            L is the list and t is 0 for cw and 1 for ccw.
            If L has no elements, finds the closest node by looking
            at all the elements on the table.
            If L has one element, finds the closest node by gossiping 
            on the other existing neighbor
            """
            closestNode = None
            if (len(L)==0):
                if (t==0):
                    closestNode = self.router.closest_successor(self.node) 
                else:
                    closestNode = self.router.closest_predecessor(self.node)
                client = WDHTClient(closestNode.ip, closestNode.port)
                if (t==0):
                    closestNode = client.closest_node_cr(self.node.id)
                else:
                    closestNode = client.closest_node_ccr(self.node.id)
                self.router.update([closestNode])

            if (len(L)==1):
                neighbors = self.router.neighbor_set.get_neighbors()
                if (t==0):
                    closestNode = self.router.closest_successor(self.node) 
                else:
                    closestNode = self.router.closest_predecessor(self.node)
                client = WDHTClient(closestNode.ip, closestNode.port)
                cur = client.gossip_neighbors(self.node.id, neighbors)
                self.neighbor_set.update(cur) 

        neighbors = self.router.neighbor_set.get_neighbors()
        isDead = set()
        for node in neighbors:
            client = WDHTClient(node.ip,node.port)
            try:
                cur = client.gossip_neighbors(self.node.id,neighbors)
                self.neighbor_set.update(cur)
            except E:
                #TODO: Check if there are other exceptions that can happen
                isDead.add(node)

        if (len(isDead)>0):
            logging.debug("Removing the neighbors %s"%(
                            ', '.join([ str(x.int_id) for x in isDead])))
            self.router.remove([x for x in isDead])

        (cw,ccw) = self.router.neighbor_set.get_candidate_list()
        while (len(cw)<2):
            helper(cw,0)
            (cw,ccw) = self.router.neighbor_set.get_candidate_list()

        while (len(ccw)<2):
            helper(ccw,1)
            (cw,ccw) = self.router.neighbor_set.get_candidate_list()

        logging.info("Maintaining Neighbors")

    def prv_maintain_routing(self):
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
        delayed_thread(lambda:handler.prv_init(existing_host, existing_port), 1)

    else:
        handler.prv_init_origin()

    # Starting maintenance threads
    periodic_thread(handler.prv_maintain_neighbors, 10)
    periodic_thread(handler.prv_maintain_routing, 30)

    # Start server
    print "Starting Server"
    start(handler, port)
    
    print("Done")
