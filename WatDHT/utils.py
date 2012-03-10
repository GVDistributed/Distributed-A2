#!/usr/bin/python

from hashlib import md5
import random
import struct
import ReadWriteLock
from threading import Lock

import itertools


def synchronize(Lock):
    def wrap(f):
        def newF(*args, **kwargs):
            lock.acquire()
            try:
                return f(*args, **kwargs)
            finally:
                lock.release()
        return newF
    return wrap

def readOnly(rwLock,timeout = None):
    def wrap(f):
        def newF(*args, **kwargs):
            rwLock.acquireRead(timeout)
            try:
                return f(*args, **kwargs)
            finally:
                rwLock.release()
        return newF
    return wrap

def writeLock(rwLock,timeout = None):
    def wrap(f):
        def newF(*args, **kwargs):
            rwLock.acquireWrite(timeout)
            try:
                return f(*args, **kwargs)
            finally:
                rwLock.release()
        return newF
    return wrap




