#!/usr/bin/python

import time

import ReadWriteLock
from threading import Lock
from threading import Thread

def wait_on(event):
    def wrap(f):
        def newF(*args, **kwargs):
            event.wait()
            return f(*args, **kwargs)
        return newF
    return wrap

def periodic_thread(func, period):
    def wrapped():
        while True:
            time.sleep(period)
            func()
    Thread(target=wrapped).start()

def delayed_thread(func, delay=0):
    def wrapped():
        time.sleep(delay)
        func()
    Thread(target=wrapped).start()

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

def writeLock(rwLock,timeout = None,callback=None):
    def wrap(f):
        def newF(*args, **kwargs):
            rwLock.acquireWrite(timeout, callback)
            try:
                return f(*args, **kwargs)
            finally:
                rwLock.release()
        return newF
    return wrap

def unique(L):
    """ Returns a unique list of nodes 
        given a list containing duplicates"""
    isHere = set([x.int_id for x in L]) 
    ret = []
    for x in L:
        if x.int_id in isHere:
            isHere.remove(x.int_id)
            ret.append(x)
    return ret



