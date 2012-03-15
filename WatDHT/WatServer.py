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
from thrift.transport.TTransport import TTransportException

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
    allow_routing = threading.Event()
    migratelock = ReadWriteLock() 

    def __init__(self, node):
        self.node = node
        self.router = Router.Router(self.node)
        self.store = Store.Store()

    @wait_on(allow_routing)
    @readOnly(migratelock)
    def get(self, key):
        next_node = self.router.route_key(key)
        if next_node.id == self.node.id:
            logging.debug("GET(%s)", key)
            value = self.store.get(key)
            if value is None:
                raise WatDHTException(WatDHTErrorType.KEY_NOT_FOUND)
            return value
        else:
            logging.debug("Sending GET(%s) to %032x", key, next_node.int_id)
            try:
                return WDHTClient(next_node.ip, next_node.port).get(key)
            except TTransportException:
                logging.info("Call to %032x failed", next_node.int_id)
                self.router.remove([next_node])
                return self.get(key) 

    @wait_on(allow_routing)
    @readOnly(migratelock)
    def put(self, key, val, duration):
        next_node = self.router.route_key(key)
        if next_node.id == self.node.id:
            logging.debug("PUT(%s, %s)", key, val)
            self.store.put(key, val, duration)
        else:
            logging.debug("Sending PUT(%s, %s) to %032x", key, val, next_node.int_id)
            try:
                WDHTClient(next_node.ip, next_node.port).put(key, val, duration)
            except TTransportException:
                logging.info("Call to %032x failed", next_node.int_id)
                self.router.remove([next_node])
                return self.put(key, val, duration)

    @wait_on(allow_routing)
    def join(self, nid):
        logging.info("%032x is Joining, Currently at %032x", nid.int_id, self.node.int_id)

        next_node = self.router.route(nid, False)
        if next_node.id == self.node.id:
            nodes = self.router.get_nodes()
        else:
            try:
                nodes = WDHTClient(next_node.ip, next_node.port).join(nid)
            except TTransportException:
                logging.info("Call to %032x failed", next_node.int_id)
                self.router.remove([next_node])
                return self.join(nid)
 
        nodes.append(self.node)
        self.router.update([nid])

        return unique(nodes)

    def ping(self):
        return self.node.id 

    @wait_on(allow_routing)
    def maintain(self, id, nid):
        ##TODO: Not sure why there is a type error sometimes... need to investigate
        logging.info("Maintain called from %032x with id %032x", nid.int_id,NodeID(id).int_id)
        closest = self.router.closest_predecessor(NodeID(id))
        if closest.id is not self.node.id:
            logging.debug(" we are not the closest making a call")
            try:
                cur = WDHTClient(closest.ip, closest.port).maintain(id, nid)
            except TTransportException:
                logging.info("Call to %032x failed", closest.int_id)
                self.router.remove([closest])
                return self.maintain(id, nid)
               
        else:
            logging.debug("We are the closest")
            cur = self.router.neighbor_set.get_neighbors()
            cur.append(self.node)

        if self.node.id != nid.id:
            # no need to update yourself
            self.router.update([nid])
        logging.debug("Maintain will return %s",' '.join(
                        ["%032x"%(x.int_id) for x in cur]))
                                        
        return cur
    
    @wait_on(allow_requests)
    @writeLock(migratelock, 0.01, lambda: WatDHTException(WatDHTErrorType.OL_MIGRATION_IN_PROGRESS))
    def migrate_kv(self, id):
        # The migratelock will time out after 0.01 seconds, in which case a migration must be
        # in progress, because it is shared with 'prv_migrate_from', and so the callback will raise
        # the given WatDHTException.

        successor = self.router.neighbor_set.get_successor()
        if successor is None:
            logging.error("No successor to %032x", self.node.int_id)
            return

        logging.info("Migrating to %032x", successor.int_id)
        if successor.id != id:
            raise WatDHTException(WatDHTErrorType.INCORRECT_MIGRATION_SOURCE, node=successor)

        # prepare and remove key-value dict from the store here
        logging.debug("The current store before migration is %s",str(self.store))
        should_migrate = lambda key: self.router.migrate_key(key, NodeID(id))
        migrating_keys = self.store.migrate_keys(should_migrate)

        logging.debug("The current store after migration is %s",str(self.store))
        return migrating_keys

    @writeLock(migratelock)
    def prv_migrate_from(self):
        # Migrate from predecessor. Shares a write lock with migrate_kv.
        backoff = 0.01
        while True:
            predecessor = self.router.neighbor_set.get_predecessor()
            if predecessor is None:
                logging.warn("No predecessor to migrate from (probably because this failed multiple times)")
                return

            logging.info("Migrating from %032x to %032x", predecessor.int_id, self.node.int_id)
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

            except TTransportException:
                logging.info("Call to %032x failed", predecessor.int_id)
                self.router.remove([predecessor])
                return self.prv_migrate_from()

        # Populate store with key values
        self.store.merge(kvs)
        logging.debug("The new store is %s", str(self.store))

    @wait_on(allow_requests)
    def gossip_neighbors(self, nid, neighbors):

        def node_find(L, x_int_id):
            for y in L:
                if  y.int_id == x_int_id:
                    return y
            return None

        logging.debug("Starting Gossip")
        cur = self.router.neighbor_set.get_neighbors()
        cur.append(self.node)
        neighbors.append(nid)
        logging.info("Calling Update from gossip")
        # Remove self from the udpate list
        neighbors.remove(self.node)
        maybe_dead = frozenset([x.int_id for x in neighbors]) - \
                    frozenset([x.int_id for x in self.router.neighbor_set.get_neighbors()])
        maybe_dead = [ node_find(neighbors, x) for x in maybe_dead]
        for node in maybe_dead:
            if node is None:
                continue
            if (node.id == self.node.id):
                continue
            try:
                WDHTClient(node.ip,node.port).ping()
            except TTransport.TTransportException as e:
                neighbors.remove(node)
                logging.exception(e)

        logging.debug("Finishing gossip by updating the neighbors %s",
                        ','.join(["%032x"%(node.int_id) for node in neighbors]))
        self.router.neighbor_set.update(neighbors)
        return cur

    @wait_on(allow_routing)
    def closest_node_cr(self, id):
        cur = self.router.closest_successor(NodeID(id))
        if cur.id == self.node.id:
            return cur
        else:
            try:
                return WDHTClient(cur.ip, cur.port).closest_node_cr(id)
            except TTransportException:
                logging.info("Call to %032x failed", cur.int_id)
                self.router.remove([cur])
                return self.closest_node_cr(id)

    @wait_on(allow_routing)
    def closest_node_ccr(self, id):
        cur = self.router.closest_predecessor(NodeID(id))
        if cur.id == self.node.id:
            return cur
        else:
            try:
                return WDHTClient(cur.ip, cur.port).closest_node_ccr(id)
            except TTransportException:
                logging.info("Call to %032x failed", cur.int_id)
                self.router.remove([cur])
                return self.closest_node_ccr(id)

    def prv_init_origin(self):
        """
        Initiate the node given that is the very first
        """
        self.allow_requests.set()
        self.allow_routing.set()
        logging.info("Routing Set")
        logging.info("Initialized")

    def prv_init(self, existing_host, existing_port):
        """
        Performs process of joining the system
        """

        # Find the predecessor and boostrap
        logging.info("Finding predecessor and bootstrapping")
        node_ids = WDHTClient(existing_host, existing_port).join(self.node) 
        self.router.update(node_ids)

        # "Enable receiving requests once join completes" is what the assignment asks for. 
        self.allow_requests.set()

        # Issue migrate_kv to its predecessor
        self.prv_migrate_from()

        # Perform routing/neighbor maintenance
        self.prv_maintain_neighbors()
        self.allow_routing.set() # only make routing decisions after fully gossiping 
        logging.info("Routing Set")
        logging.info("Calling Routing from initialize")
        self.prv_maintain_routing()

        logging.info("Initialized")

    @wait_on(allow_requests)
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
        
        # Find out who is dead.
        neighbors = self.router.neighbor_set.get_neighbors()
        isDead = []
        for node in neighbors:
            client = WDHTClient(node.ip, node.port)
            try:
                cur = client.gossip_neighbors(self.node,neighbors)
                to_update = [node for node in cur if node.id != self.node.id]
                self.router.neighbor_set.update(to_update)
            except TTransportException as e:
                isDead.append(node)
                logging.exception(e)
        isDead = unique(isDead)

        if (len(isDead)>0):
            logging.debug("Removing the neighbors %s"%(
                            ', '.join([ str(x.int_id) for x in isDead])))
            self.router.remove([x for x in isDead])

        ### Add the proper node to each side if we are mising a node.
        logging.debug("Going to do CW side")
        (cw,ccw) = self.router.neighbor_set.get_candidate_list()
        tries =0
        while (len(cw)<2) and tries<2:
            helper(cw, 0)
            (cw,ccw) = self.router.neighbor_set.get_candidate_list()
            tries +=1 

        logging.debug("Going to do CCW side")
        tries = 0
        while (len(ccw)<2) and tries<2:
            helper(ccw,1)
            (cw,ccw) = self.router.neighbor_set.get_candidate_list()
            tries +=1 

        logging.info("Done Maintaining Neighbors")
        self.router.neighbor_set.debug()

    @wait_on(allow_routing)
    def prv_maintain_routing(self):
        """
        Performs periodic routing maintenance by pinging
        """
        logging.info("Maintaining Routing Table")
        neighbors = self.router.routing_table.get_nodes()
        isDead = []
        for node in neighbors:
            try:
                logging.debug("Going to ping %032x",node.int_id) 
                WDHTClient(node.ip, node.port).ping()
            except TTransportException:
                isDead.append(node)
                logging.exception(e)
        isDead = unique(isDead)
        self.router.remove(isDead)
        missing_regions = self.router.routing_table.get_missing_regions()
        for r,val in missing_regions.iteritems():
            logging.info("I am %032x. Going to call maintain on %032x, with length %s",self.node.int_id, val, len(NodeID.to_id(val)))
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
    logging.basicConfig(filename = 'log', format = logging_format, level=logging.DEBUG)
    logging.info("------------------------STARTING RUN-----------------------------------")
    if not len(sys.argv) in (4, 6):
        print "Usage: ./server node_id ip port [existing_ip existing_port]"
        sys.exit(-1)

    logging.info("Initializing ...")
    node_id = Router.hash(sys.argv[1])
    host = sys.argv[2]
    port = int(sys.argv[3])    
    node = NodeID(node_id, host, port)

    logging.info("Node: %x (%s:%d)", node.int_id, host, port)
    handler = WDHTHandler(node)

    if len(sys.argv) == 6:
        existing_host = sys.argv[4]
        existing_port = int(sys.argv[5])

        logging.info("Joining (%s:%d)", existing_host, existing_port)
        delayed_thread(lambda:handler.prv_init(existing_host, existing_port))

    else:
        handler.prv_init_origin()

    # Starting maintenance threads
    periodic_thread(handler.prv_maintain_neighbors, 10)
    periodic_thread(handler.prv_maintain_routing, 30)

    # Start server
    logging.info("Starting Server")
    start(handler, port)
    logging.info("Done") 
