#!/usr/bin/python

import time
from utils import readOnly, writeLock
from ReadWriteLock import ReadWriteLock
from ttypes import NodeID

class Store(object):

    StoreLock = ReadWriteLock()

    def __init__(self):
        self.store = dict()
    
    @readOnly(StoreLock)
    def get(self, key):
        """ Returns a value given a key """
        value, expiry = self.store.get(key, (None, None)) 

        if expiry is not None and time.time() > expiry:
            try:
                self.store.pop(key)
            except KeyError:
                pass # race condition
            return None

        return value
                
    @writeLock(StoreLock)
    def put(self, key, value, expiry):
        if expiry < 0:
            expiry = None
        elif expiry == 0:
            if self.store.has_key(key):
                self.store.pop(key)
            return 
        else:
             expiry = time.time() + expiry
        
        self.store[key] = (value, expiry)

    @writeLock(StoreLock)
    def migrate_keys(self, node):
        """ Returns a dict of keys that are greater than an node
            and returns them from the dictionary """
        ret = dict()
        thresh = node.int_id
        for k,(v,e) in self.store.iteritems():
            if (k>=thresh):
                ret[k]=v
        for k in ret.iterkeys():
            self.store.pop(k)
        return ret

    @writeLock(StoreLock)
    def merge(self, kvstore):
        for k,v in kvstore.iteritems():
            self.store[k] = (v,None)
    
    @readOnly(StoreLock)
    def __str__(self):
        return "{"+ ' '.join(["<%s::%s,%s>"%(k,v,str(e))
                             for k,(v,e) in self.store.iteritems()]) + "}"
