#!/usr/bin/env python

import time
import os
from utils import delayed_thread
from WatClient import WDHTClient

import random

num_hosts = 10
num_pairs = 1000
first_port = 11333

def rand_string():
    return ''.join(chr(random.randrange(ord('A'), ord('z')+1)) for i in range(random.randrange(5, 31)))

table = []
for i in range(num_pairs):
    table.append((rand_string(), rand_string()))

try:
    print "[[Starting 0]]"
    delayed_thread(lambda: os.system("./server 0 localhost %d" % first_port))
    for i in range(1, num_hosts):
        # time.sleep(1)
        print "[[Starting %d]]" % i
        delayed_thread((lambda i: lambda: os.system("./server %d localhost %d localhost %d" % (i, first_port + i, first_port)))(i))
    time.sleep(1)

    for i in range(num_pairs):
        key, value = table[i]
        print "[[Inserting (%s, %s)]]" % (key, value)
        WDHTClient("localhost", random.randrange(num_hosts) + first_port).put(key, value, -1)

    for i in range(num_pairs):
        key, value = table[i]
        print "[[Getting %s]]" % key
        retrieved_value = WDHTClient("localhost", random.randrange(num_hosts) + first_port).get(key)
        print "[[Got %s =? %s (%s)]]" % (retrieved_value, value, retrieved_value == value)
        if value != retrieved_value: 
            print "Failed"
            raise Exception

    print "Passed!"

except KeyboardInterrupt:
    os.system("./kill-server")
finally:
    os.system("./kill-server")
