#!/usr/bin/env python

import time
import os
from utils import delayed_thread
from WatClient import WDHTClient

try:
    delayed_thread(lambda: os.system("./server aaron localhost 15247"))
    delayed_thread(lambda: os.system("./server guru localhost 15246 localhost 15247"), 0.1)
    time.sleep(1)

    print "Putting"
    WDHTClient("localhost", 15246).put("XX", "1", -1)
    WDHTClient("localhost", 15246).put("YY", "2", -1)
    WDHTClient("localhost", 15246).put("ZZ", "3", -1)

    print "Killing one"
    os.system("./kill-one-server aaron")
    time.sleep(0.1)

    print "Finishing"
    WDHTClient("localhost", 15246).put("a", "4", -1)
    WDHTClient("localhost", 15246).put("b", "5", -1)
    print WDHTClient("localhost", 15246).get("a")
    print WDHTClient("localhost", 15246).get("b")

except KeyboardInterrupt:
    os.system("./kill-server")
finally:
    os.system("./kill-server")
