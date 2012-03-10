#!/usr/bin/env python

import time
import os
from utils import delayed_thread
from WatClient import WDHTClient

delayed_thread(lambda: os.system("./server 0 localhost 15243"))

delayed_thread(lambda: os.system("./server 1 localhost 12544 localhost 12543"), 0.5)

time.sleep(1)

WDHTClient("localhost", 15243).put("a", "b", -1)
print WDHTClient("localhost", 12544).get("a")

os.system("./kill-server")
