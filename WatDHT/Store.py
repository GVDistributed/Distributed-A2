#!/usr/bin/python

from utils import readOnly, writeLock
import ReadWriteLock
import ttypes

StoreLock = ReadWriteLock()
class Store():

        def __init__(self):
            self.dataStore = dict()

    
        @readOnly(StoreLock)
        def get(self,key)
            """ Returns a value given a key """
            return self.dataStore.get(key,None) 
                
        @writeLock(StoreLock)
        def put(self, key, value)
            self.dataStore[key]=value
