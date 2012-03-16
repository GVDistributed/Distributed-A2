#!/usr/bin/env python

import time
import os
import random

from utils import delayed_thread
from WatClient import WDHTClient
from ttypes import WatDHTException
num_hosts = 100
to_del = 50
num_pairs = 1000
first_port = 16208

def rand_string():
    return ''.join(chr(random.randrange(ord('A'), ord('z')+1)) for i in range(random.randrange(5, 31)))

table = []
for i in range(num_pairs):
    table.append((rand_string(), rand_string()))


try:
    delayed_thread(lambda: os.system("./server aaron0 localhost %d" % first_port ))
    time.sleep(1)

    for i in xrange(1, num_hosts):
        print("[[Starting %d]]" % i)
        delayed_thread((lambda i: lambda: os.system("./server aaron%d localhost %d localhost %d" % (i, first_port + i, first_port)))(i))

    time.sleep(10)
    print "Putting"
    last_index = num_pairs/2

    for i in xrange(last_index):
        key, value = table[i]
        print "[[Inserting (%s, %s)]]" % (key, value)
        WDHTClient("localhost", random.randrange(num_hosts) + first_port).put(key, value, -1)


    for i in xrange(to_del):
        print("[[Killing %d]]"%i)
        os.system("./kill-one-server aaron%d"%i)
    time.sleep(10)

    for i in xrange(last_index,num_pairs):
        key, value = table[i]
        print "[[Inserting (%s, %s)]]" % (key, value)
        WDHTClient("localhost", max(to_del, random.randrange(num_hosts)) + first_port).put(key, value, -1)

    fail =0
    for i in range(num_pairs):
        key, value = table[i]
        print "[[Getting %s]]" % key
        try:
            retrieved_value = WDHTClient("localhost", max(to_del, random.randrange(num_hosts)) + first_port).get(key)
        except WatDHTException as e:
            print e.error_code, e.error_message, e.node
            fail+=1
            continue
        print "[[Got %s =? %s (%s)]]" % (retrieved_value, value, retrieved_value == value)
        if value != retrieved_value: 
            print "Failed"
            raise Exception
    print("Passed")
    print(fail,num_pairs)


except KeyboardInterrupt:
    os.system("./kill-server")
finally:
    os.system("./kill-server")
