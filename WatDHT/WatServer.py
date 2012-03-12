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
from utils import unique

from ReadWriteLock import ReadWriteLock
from WatClient import WDHTClient

import Store
import Router

class WDHTHandler(Iface):

    allow_requests = threading.Event()
    migratelock = ReadWriteLock() 

    def __init__(self, node):
        self.node = node
        self.router = Router.Router(self.node)
        self.store = Store.Store()

    @wait_on(allow_requests)
    @readOnly(migratelock)
    def get(self, key):
        next_node = self.router.route_key(key)
        if next_node is None:
            logging.debug("GET(%s)", key)
            value = self.store.get(key)
            if value is None:
                raise WatDHTException(WatDHTErrorType.KEY_NOT_FOUND)
            return value
        else:
            logging.debug("Sending GET(%s) to %032x", key, next_node.int_id)
        return WDHTClient(next_node.ip, next_node.port).get(key)

    @wait_on(allow_requests)
    @readOnly(migratelock)
    def put(self, key, val, duration):
        next_node = self.router.route_key(key)
        if next_node is None:
            logging.debug("PUT(%s, %s)", key, val)
            self.store.put(key, val, duration)
        else:
            logging.debug("Sending PUT(%s, %s) to %032x", key, val, next_node.int_id)
            WDHTClient(next_node.ip, next_node.port).put(key, val, duration)

    @wait_on(allow_requests)
    def join(self, nid):
        """
        Parameters:
         - nid
        """
        logging.info("%032x is Joining, Currently at %032x", nid.int_id, self.node.int_id)

        next_node = self.router.route(nid, False)
        if next_node is None:
            nodes = self.router.get_nodes()
        else:
            nodes = WDHTClient(next_node.ip, next_node.port).join(nid)
        nodes.append(self.node)

        self.router.update([nid])
        #logging.debug(str(self.router))

        return unique(nodes)

    def ping(self):
        return self.node.id 

    @wait_on(allow_requests)
    def maintain(self, id, nid):
        ## TODO: Not sure why there is a type error sometimes... need to investigate
        #        try:
        #            logging.info("Maintain called from %032x with id %032x", nid.int_id,NodeID.to_id(id))
        #        except TypeError:
        #            print(id,len(id),"%032x"%nid.int_id, "%032x"%self.node.int_id)
        #
        cur = None
        closest = self.router.closest_predecessor(NodeID(id,-1,-1))
        logging.debug("Closest id was %032x",closest.int_id)
        if closest.id is not self.node.id:
            logging.debug(" we are not the closest making a call")
            client = WDHTClient(closest.ip, closest.port)
            cur = client.maintain(id,nid)
        else:
            logging.debug("we are the closest")
            cur = self.router.neighbor_set.get_neighbors()
            cur.append(self.node)
        if (self.node.id != nid.id):
            #no need to update yourself
            self.router.update([nid])
        logging.debug("Maintain will return %s",' '.join(
                        ["%032x"%(x.int_id) for x in cur]))
                                        
        return cur

    @wait_on(allow_requests)
    @writeLock(migratelock, 0.01, lambda: WatDHTException(WatDHTErrorType.OL_MIGRATION_IN_PROGRESS))
    def migrate_kv(self, id):
        successor = self.router.neighbor_set.get_successor()
        logging.info("Migrating to %032x", successor.int_id)
        if successor.id != id:
            raise WatDHTException(WatDHTErrorType.INCORRECT_MIGRATION_SOURCE, node=successor)

        # prepare and remove key-value dict from the store here
        logging.debug("The current store before migration is %s",str(self.store))
        migrating_keys = self.store.migrate_keys(NodeID(id,-1,-1))
        logging.debug("The current store after migration is %s",str(self.store))
        return migrating_keys

    @writeLock(migratelock)
    def prv_migrate_from(self):
        # Migrate from predecessor
        backoff = 0.01
        while True:
            predecessor = self.router.neighbor_set.get_predecessor()
            logging.info("Migrating from %032x to %032x", predecessor.int_id, self.node.int_id)
            try:
                kvs = WDHTClient(predecessor.ip, predecessor.port).migrate_kv(self.node.id)
                break

            except WatDHTException, e:
                if e.error_code == WatDHTErrorType.INCORRECT_MIGRATION_SOURCE:
                    self.router.update([e.node])
                    logging.debug("Incorrect migration source")
                    # print "%032x %032x %032x" % (self.node.int_id, self.router.neighbor_set.get_predecessor().int_id, e.node.int_id)
                    # assert self.router.neighbor_set.get_predecessor().id == e.node.id
                elif e.error_code == WatDHTErrorType.OL_MIGRATION_IN_PROGRESS:
                    backoff *= 1.1
                    time.sleep(backoff)
                    logging.debug("Backing off")
                else:
                    raise

        # Populate store with key values
        self.store.merge(kvs)
        logging.debug("The new store is %s", str(self.store))

    @wait_on(allow_requests)
    def gossip_neighbors(self, nid, neighbors):
        """
        Parameters:
         - nid
         - neighbors
        """
        cur = self.router.neighbor_set.get_neighbors()
        cur.append(self.node)
        neighbors.append(nid)
        logging.info("Calling Update from gossip")
        #Remove self from the udpate list
        neighbors.remove(self.node)
        self.router.neighbor_set.update(neighbors)
        return cur

    @wait_on(allow_requests)
    def closest_node_cr(self, id):
        """
        Parameters:
         - id
        """
        cur = self.router.closest_successor(NodeID(id,-1,-1))
        if cur.id == self.node.id:
            return cur
        else:
            client = WDHTClient(cur.ip, cur.port)
            return client.closest_node_cr(id)

    @wait_on(allow_requests)
    def closest_node_ccr(self, id):
        """
        Parameters:
         - id
        """
        cur = self.router.closest_predecessor(NodeID(id,-1,-1))
        if cur.id == self.node.id:
            return cur
        else:
            return WDHTClient(cur.ip, cur.port).closest_predecessor(id)

    def prv_init_origin(self):
        """
        Initiate the node given that it's the very first
        """
        logging.info("Done Bootstrapping")
        self.allow_requests.set()

    def prv_init(self, existing_host, existing_port):
        """
        Performs process of joining the system
        """

        # Find the predecessor and boostrap
        logging.info("Finding predecessor and bootstrapping")
        node_ids = WDHTClient(existing_host, existing_port).join(self.node) 
        self.router.update(node_ids)
        # print "%032x:%d" % (self.node.int_id, self.node.port)
        # print str(self.router.neighbor_set)

        # Issue migrate_kv to its predecessor
        self.prv_migrate_from()

        # "Enable receiving requests once join completes" is what the assignment asks for. 
        # But we'll ignore this and enable it after migration
        self.allow_requests.set()

        # Perform routing/neighbor maintenance
        self.prv_maintain_neighbors()
        self.prv_maintain_routing()
        
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
                if closestNode is None:
                    logging.debug("The closest node was None")

                if closestNode.id != self.node.id:
                    self.router.update([closestNode])

            if (len(L)==1):
                neighbors = self.router.neighbor_set.get_neighbors()
                if (t==0):
                    closestNode = self.router.closest_successor(self.node) 
                else:
                    closestNode = self.router.closest_predecessor(self.node)

                if (closestNode.id != self.node.id):
                    client = WDHTClient(closestNode.ip, closestNode.port)
                    cur = client.gossip_neighbors(self.node, neighbors)
                    cur.remove(self.node)
                    self.router.neighbor_set.update(cur) 

        ##Find out who is dead.
        neighbors = self.router.neighbor_set.get_neighbors()
        isDead = []
        for node in neighbors:
            client = WDHTClient(node.ip,node.port)
            try:
                cur = client.gossip_neighbors(self.node,neighbors)
                to_update = [node for node in cur if node.id != self.node.id]
                self.router.neighbor_set.update(to_update)
            except Exception as e:
                #TODO: Check if there are other exceptions that can happen
                isDead.append(node)
                print e
        isDead = unique(isDead)

        if (len(isDead)>0):
            logging.debug("Removing the neighbors %s"%(
                            ', '.join([ str(x.int_id) for x in isDead])))
            self.router.remove([x for x in isDead])

        ### Add the proper node to each side if we are mising a node.
        (cw,ccw) = self.router.neighbor_set.get_candidate_list()
        while (len(cw)<2):
            helper(cw,0)
            (cw,ccw) = self.router.neighbor_set.get_candidate_list()

        while (len(ccw)<2):
            helper(ccw,1)
            (cw,ccw) = self.router.neighbor_set.get_candidate_list()

        logging.info("Done Maintaining Neighbors")
        self.router.neighbor_set.debug()

    def prv_maintain_routing(self):
        #### SKIPPING FOR NOW
        return None
        """
        Performs periodic routing maintenance by pinging
        """
        logging.info("Maintaining Routing Table")
        neighbors = self.router.routing_table.get_nodes()
        isDead = set()
        for node in neighbors:
            try:
                WDHTClient(node.ip, node.port).ping()
            except Exception as e:
                isDead.add(node)

        self.router.remove([x for x in isDead])
        missing_regions = self.router.routing_table.get_missing_regions()
        for r,val in missing_regions.iteritems():
            self.maintain(NodeID.to_id(val),self.node)
        logging.info("Done Maintaing Routing Table")
        self.router.routing_table.debug() 
         
def start(handler, port):
    processor = Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    server.serve()

if __name__ == '__main__':
    
    logging_format = '%(asctime)s %(process)04d %(levelname)5s %(message)s'
    logging.basicConfig(filename = 'log', format = logging_format, level=logging.DEBUG)#INFO)
    logging.info("------------------------STARTING RUN-----------------------------------")
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
        delayed_thread(lambda:handler.prv_init(existing_host, existing_port))

    else:
        handler.prv_init_origin()

    # Starting maintenance threads
    #TODO Change this back to 10s and 30 s
    periodic_thread(handler.prv_maintain_neighbors, 1)
    periodic_thread(handler.prv_maintain_routing, 5)

    # Start server
    print "Starting Server"
    start(handler, port)
    
    print("Done")
