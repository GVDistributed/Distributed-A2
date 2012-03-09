#!/usr/bin/env python
 
import sys
sys.path.append('../gen-py')
sys.path.append('/u5/gguruganesh/cs454/Distributed-A2/lib/python2.6/site-packages/')
 
from WatDHT import Client
from constants import *
from ttypes import *
 
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
 
try:
  # Make socket
  transport = TSocket.TSocket('localhost', 9090)
 
  # Buffering is critical. Raw sockets are very slow
  transport = TTransport.TBufferedTransport(transport)
 
  # Wrap in a protocol
  protocol = TBinaryProtocol.TBinaryProtocol(transport)
 
  # Create a client to use the protocol encoder
  client = Client(protocol)
 
  # Connect!
  transport.open()  
  print("Going to ping") 
  for i in xrange(1000000):
    s= client.get(sys.argv[1])
    print(s)
  print("Done pinging")

  transport.close()
 
except Thrift.TException, tx:
  print "%s" % (tx.message)
