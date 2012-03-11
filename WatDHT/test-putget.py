#!/usr/bin/env python

import time
import os
from utils import delayed_thread
from WatClient import WDHTClient
try:
    delayed_thread(lambda: os.system("./server aaron localhost 15243"))

    delayed_thread(lambda: os.system("./server guru localhost 15244 localhost 15243"), 0.5)

    time.sleep(0.6)

    WDHTClient("localhost", 15243).put("XX", "1", -1)
    WDHTClient("localhost", 15243).put("YY", "2", -1)
    WDHTClient("localhost", 15243).put("ZZ", "3", -1)
    WDHTClient("localhost", 15243).put("a", "4", -1)
    WDHTClient("localhost", 15243).put("b", "5", -1)
    delayed_thread(lambda: os.system("./server aarom localhost 15245 localhost 15243"), 0.5)
    WDHTClient("localhost", 15243).put("c", "6", -1)
    print WDHTClient("localhost", 15244).get("a")

    time.sleep(2) 
except KeyboardInterrupt:
    os.system("./kill-server")
finally:
    os.system("./kill-server")
