#!/usr/bin/python

import time
from utils import readOnly, writeLock
from ReadWriteLock import ReadWriteLock

class Store(object):

    storelock = ReadWriteLock()

    def __init__(self):
        self.store = dict()
    
    @readOnly(storelock)
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
                
    @writeLock(storelock)
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
