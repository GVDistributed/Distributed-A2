#!/usr/bin/python

from hashlib import md5
import random
import struct

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

    def __init__(self, node, logm, regions):
        self.node = node
        self.logm = logm
        self.regions = regions
        self.table = {}

    def update(self, nodes):
        for node in nodes:
            self.add_node(node)

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
        rel_id = (node.int_id - self.node.int_id) % (1 << self.logm)
        for r in range(self.regions - 1):
            if rel_id & (1 << (self.logm - r - 1)):
                return r
        return self.regions - 1

    def debug(self):
        for r in range(self.regions):
            if self.table.has_key(r):
                print "%d %032x" % (r, self.table[r].int_id)
            else:
                print r, "None"

class NeighborSet(object):
    
    def __init__(self, node, size):
        self.node = node
        self.size = size
        self.ccw = []
        self.cw = []

    def is_neighbor(self, node):
        for neighbor in itertools.chain(self.cw, self.ccw):
             if neighbor.int_id == node.int_id:
                 return True
        return False
        
    def get_neighbors(self):
        return self.ccw + self.cw

    '''
    def update(self, nodes):
        for node in nodes:
            self.add_neighbor(node)
            
    def add_neighbor(self, node):
        if node.int_id > self.node.int_id:
            lis = self.cw
        else:
            lis = self.ccw
        lis.append(node)
        lis.sort(key = lambda x: x.int_id )
        if len(lis) > self.size:
            lis.pop()    
    '''
        
    def debug(self):
        print "CCW:", ', '.join("%032x" % node.int_id for node in reversed(self.ccw))
        print "CW: ", ', '.join("%032x" % node.int_id for node in self.cw)

class Router(object):
    pass

if __name__ == '__main__':

    m0 = NodeID.dummy()
    m1 = NodeID.dummy()
    m2 = NodeID.dummy()
    m3 = NodeID.dummy()
    m4 = NodeID.dummy()
    m5 = NodeID.dummy()
    m6 = NodeID.dummy()
    
    #m0.id = struct.pack(">QQ", 0, 0)
    
    routing_table = RoutingTable(m0, 128, 128 / 32)
    routing_table.update([m1, m2, m3, m4, m5, m6])

    routing_table.debug()

    neighbor_set = NeighborSet(m0, 2)
    
    neighbor_set.update([m1, m2, m3])
    print neighbor_set.debug()
    neighbor_set.update([m1, m2, m3])
    print neighbor_set.debug()
