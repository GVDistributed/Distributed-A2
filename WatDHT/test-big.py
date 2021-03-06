#!/usr/bin/env python

import time
import os
import random

from utils import delayed_thread
from WatClient import WDHTClient
from ttypes import WatDHTException

num_hosts = 100
num_pairs = 1000
first_port = 16208

def rand_string():
    return ''.join(chr(random.randrange(ord('A'), ord('z')+1)) for i in range(random.randrange(5, 31)))

table = []
for i in range(num_pairs):
    table.append((rand_string(), rand_string()))

try:
    print "[[Starting 0]]"
    delayed_thread(lambda: os.system("./server 0 localhost %d" % first_port))
    time.sleep(2)
    for i in range(1, num_hosts):
        # time.sleep(1)
        print "[[Starting %d]]" % i
        delayed_thread((lambda i: lambda: os.system("./server %d localhost %d localhost %d" % (i, first_port + i, first_port)))(i))
    time.sleep(30)

    for i in range(num_pairs):
        key, value = table[i]
        print "[[Inserting (%s, %s)]]" % (key, value)
        WDHTClient("localhost", random.randrange(num_hosts) + first_port).put(key, value, -1)

    for i in range(num_pairs):
        key, value = table[i]
        print "[[Getting %s]]" % key
        try:
            retrieved_value = WDHTClient("localhost", random.randrange(num_hosts) + first_port).get(key)
        except WatDHTException as e:
            print e.error_code, e.error_message, e.node
            raise 
        print "[[Got %s =? %s (%s)]]" % (retrieved_value, value, retrieved_value == value)
        if value != retrieved_value: 
            print "Failed"
            raise Exception

    print "Passed!"

except KeyboardInterrupt:
    os.system("./kill-server")
finally:
    os.system("./kill-server")
