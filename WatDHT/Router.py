#!/usr/bin/python

from hashlib import md5
import random
import struct

from utils import readOnly, writeLock, unique
from ReadWriteLock import ReadWriteLock

import logging
import itertools

from ttypes import NodeID

class DummyNodeID(object):
    """
    Dummy class for testing purposes
    """
    @classmethod
    def dummy(cls):
        obj = cls()
        obj.id = hash(str(random.randrange(1<<30)))
        return obj

    @property
    def int_id(self):
        x = struct.unpack(">QQ", self.id)
        return (x[0]<<64)|x[1]

def hash(x):
    return md5(x).digest()

class RoutingTable(object):
    RoutingTableLock = ReadWriteLock() 
    def __init__(self, node, logm, regions):
        self.node = node
        self.logm = logm
        self.regions = regions
        self.table = {}

    @writeLock(RoutingTableLock)
    def remove(self, nodes):
        toDel = set()
        for node in nodes:
            for k,v in self.table.iteritems():
                if (v.id == node.id):
                    toDel.add(k)
        for x in toDel:
            self.table.pop(k)
        
    @writeLock(RoutingTableLock)
    def update(self, nodes):
        for node in nodes:
            self.add_node(node)


    @writeLock(RoutingTableLock)
    def add_node(self, node):
        """
            Adds a new node to the routing table.
            If there was already a node in the corresponding region
            then this replaces it and returns True. Otherwise False.
        """
        r = self.get_region(node)
        was_replaced = not self.table.has_key(r)
        self.table[r] = node
        return was_replaced

    
    def get_region(self, node):
        diff_bits = self.node.int_id ^ node.int_id
        for r in range(self.regions - 1):
            mask = (1 << (self.logm - r - 1))
            if diff_bits & mask:
                return r
        return self.regions - 1

    def get_last_node(self, r):
        ret = self.node.int_id
        ret = ret | ((1<< (self.logm-r-1))-1)
        ret = ret ^ (1<< (self.logm-r-1))
        return ret

    @readOnly(RoutingTableLock)
    def get_missing_regions(self):
        ret = dict()
        for r in xrange(self.regions):
            if r not in self.table:
                ret[r]= self.get_last_node(r)
        return ret
                

    @readOnly(RoutingTableLock)
    def get_nodes(self):
        return unique(self.table.values())
    
    @readOnly(RoutingTableLock)
    def debug(self):
        for r in range(self.regions):
            if self.table.has_key(r):
                logging.debug("%d %032x", r, self.table[r].int_id)
            else:
                logging.debug("%d", r)

    @readOnly(RoutingTableLock)
    def __str__(self):
        ret = "Routing Table\n"
        for r in xrange(self.regions):
            if self.table.has_key(r):
                ret+= "%d %032x\n"%(r,self.table[r].int_id)
        return ret

def cw_distance(origin, p, mod):
    return (p.int_id - origin.int_id) % mod

def ccw_distance(origin, p, mod):
    return (origin.int_id - p.int_id) % mod

def distance(p1, p2, mod):
    return min(cw_distance(p1, p2, mod), 
               ccw_distance(p1, p2, mod))

class NeighborSet(object):
    NeighborLock = ReadWriteLock() 
    def __init__(self, node, size, m):
        self.node = node
        self.size = size
        self.cw = []
        self.ccw = []
        self.m = m

    @readOnly(NeighborLock)
    def is_neighbor(self, node):
        for neighbor in itertools.chain(self.cw, self.ccw):
             if neighbor.int_id == node.int_id:
                 return True
        return False
        
    def cw_distance(self, node):
        return cw_distance(self.node, node, self.m)

    def ccw_distance(self, node):
        return ccw_distance(self.node, node, self.m)
    
    @readOnly(NeighborLock)
    def get_neighbors(self):
        return unique(self.cw + self.ccw)

    @readOnly(NeighborLock)
    def get_successor(self):
        if not self.cw:
            return None
        return self.cw[0]

    @readOnly(NeighborLock)
    def get_predecessor(self):
        if not self.ccw:
            return None
        return self.ccw[0]

    @writeLock(NeighborLock)
    def remove(self, nodes):
        for node in nodes:
            self.cw.remove(node)
            self.ccw.remove(node)

    @readOnly(NeighborLock)
    def get_candidate_list(self):
        return (self.cw[:], self.ccw[:])
        

    @writeLock(NeighborLock)
    def update(self, nodes):
        logging.debug("Adding nodes %s",' '.join(
                            ["%032x"%(x.int_id) for x in nodes]))
        self.cw.extend(nodes)
        self.ccw.extend(nodes)
        self.cw = unique(self.cw)
        self.ccw = unique(self.ccw)
        self.cw.sort(key = self.cw_distance)
        self.ccw.sort(key = self.ccw_distance)
        self.cw = self.cw[:self.size]
        self.ccw = self.ccw[:self.size]
 
    @readOnly(NeighborLock)
    def debug(self):
        logging.debug("CW: " + ', '.join("%032x" % node.int_id for node in self.cw))
        logging.debug("CCW:" + ', '.join("%032x" % node.int_id for node in self.ccw))

    @readOnly(NeighborLock)
    def __str__(self):
        ret = "NeighorSet\n"+ "CW: " + ', '.join("%032x" % node.int_id for node in self.cw) + "\nCCW:" + ', '.join("%032x" % node.int_id for node in self.ccw) + "\n"
        return ret

class Router(object):
    RouterLock = ReadWriteLock()
    def __init__(self, node, n = 128):
        self.n = n 
        self.m = 2**self.n
        self.routing_table = RoutingTable(node, self.n, self.n / 32)
        self.neighbor_set = NeighborSet(node, 2, self.m)
        self.node = node

    @readOnly(RouterLock)
    def distance(self, node):
        return distance(self.node, node, self.m)

    @readOnly(RouterLock)
    def get_nodes(self):
        return unique(self.routing_table.get_nodes() + self.neighbor_set.get_neighbors())

    @readOnly(RouterLock)
    def candidates(self):
        return unique([self.node] + self.get_nodes())

    @readOnly(RouterLock)
    def closest_predecessor(self, node):
        """ returns the closest node in the ccw distance"""
        return min(self.candidates(),
                   key = lambda p: ccw_distance(node, p, self.m))
    
    @readOnly(RouterLock)
    def closest_successor(self, node):
        """ returns the closest node in the cw distance"""
        return min(self.candidates(),
                    key = lambda p: cw_distance(node, p, self.m))

    @readOnly(RouterLock)
    def closest_absolute_node(self, node):
        return min(self.candidates(),
                   key = lambda p: distance(node, p, self.m))

    @readOnly(RouterLock)
    def route_key(self, key):
        nid = hash(key)
        target_node = NodeID(nid, -1, -1)
        return self.route(target_node)

    @readOnly(RouterLock)
    def route(self, node, inclusive=True):
        """
        Routes to the closest predecessor. May route along either direction
        according to whichever minimizes the absolute distance. Case logic
        is necessary to recognize when we may have just overshot the
        closest predecessor.
        """

        if not inclusive:
            # decrease the node id by one
            node = NodeID(NodeID.to_id((node.int_id - 1) %  self.m), -1, -1)

        closest_predecessor = self.closest_predecessor(node)
        closest_node = self.closest_absolute_node(node)
        predecessor = self.neighbor_set.get_predecessor()

        # self.debug()
        # logging.debug("%032x %032x %032x", self.node.int_id, closest_predecessor.int_id, closest_node.int_id)

        if closest_predecessor.id == self.node.id:
            # we happen to be the closest predecessor
            return None
 
        elif closest_node.id == self.node.id:
            # we happen to be the closest node, but not the closest predecessor, so it must be true that
            # our predecessor is the closest predecessor
            assert predecessor.id != self.node.id
            return predecessor

        return closest_node

    @writeLock(RouterLock)
    def update(self, nodes):
        self.routing_table.update(nodes)
        self.neighbor_set.update(nodes)

    @writeLock(RouterLock)
    def remove(self, nodes):
        self.routing_table.remove(nodes)
        self.neighbor_set.remove(nodes)

    @readOnly(RouterLock)
    def debug(self):
        logging.debug("{Routing Table}")
        self.routing_table.debug()
        logging.debug("{Neighbor Set}")
        self.neighbor_set.debug()
    
    @readOnly(RouterLock)
    def __str__(self):
        ret = "Router\n\n %s\n %s\n"%(
                str(self.neighbor_set), str(self.routing_table))
        return ret


if __name__ == '__main__':
    m0 = DummyNodeID.dummy()
    m1 = DummyNodeID.dummy()
    m2 = DummyNodeID.dummy()
    m3 = DummyNodeID.dummy()
    m4 = DummyNodeID.dummy()
    m5 = DummyNodeID.dummy()
    m6 = DummyNodeID.dummy()
    
    m0.id = struct.pack(">QQ", 0, 0)
   
    router = Router(m0)

    routing_table = router.routing_table
    routing_table.update([m1, m2, m3, m4, m5, m6])
    routing_table.debug()

    neighbor_set = router.neighbor_set
    
    neighbor_set.update([m1])
    neighbor_set.debug()
    neighbor_set.update([m2, m3])
    neighbor_set.debug()
    neighbor_set.update([m4, m5, m6])
    neighbor_set.debug()

    for i in range(10):
        m = DummyNodeID.dummy()
        print "%032x %032x" % (m.int_id, router.closest_node(m).int_id)

