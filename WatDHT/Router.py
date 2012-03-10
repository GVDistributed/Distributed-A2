#!/usr/bin/python

from hashlib import md5
import random
import struct
from utils import readOnly, writeLock
from ReadWriteLock import ReadWriteLock
import itertools

class NodeID(object):
    # binary id
    # string ip
    # i32 port
   
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

    @readOnly(RoutingTableLock)
    def get_nodes(self):
        return self.table.values()
    
    @readOnly(RoutingTableLock)
    def debug(self):
        print "[%x]" % self.node.int_id
        for r in range(self.regions):
            if self.table.has_key(r):
                print "%d %032x" % (r, self.table[r].int_id)
            else:
                print r, "None"

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
        return self.cw + self.ccw

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
    def update(self, nodes):
        self.cw.extend(nodes)
        self.ccw.extend(nodes)
        self.cw.sort(key = self.cw_distance)
        self.ccw.sort(key = self.ccw_distance)
        self.cw = self.cw[:self.size]
        self.ccw = self.ccw[:self.size]
 
    @readOnly(NeighborLock)
    def debug(self):
        print "CW: ", ', '.join("%032x" % node.int_id for node in self.cw)
        print "CCW:", ', '.join("%032x" % node.int_id for node in self.ccw)

class Router(object):
    RouterLock = ReadWriteLock()
    def __init__(self, node, n = 128):
        self.n = n 
        self.routing_table = RoutingTable(node, self.n, self.n / 32)
        self.neighbor_set = NeighborSet(node, 2, 2**self.n)
        self.node = node
    
    @readOnly(RouterLock)
    def closest_node(self, node):
        return min(self.routing_table.get_nodes() + self.neighbor_set.get_neighbors(),
                   key = lambda p: distance(node, p, 2**self.n))

if __name__ == '__main__':

    m0 = NodeID.dummy()
    m1 = NodeID.dummy()
    m2 = NodeID.dummy()
    m3 = NodeID.dummy()
    m4 = NodeID.dummy()
    m5 = NodeID.dummy()
    m6 = NodeID.dummy()
    
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
        m = NodeID.dummy()
        print "%032x %032x" % (m.int_id, router.closest_node(m).int_id)
