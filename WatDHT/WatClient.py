import sys

from WatDHT import Client
from constants import *
from ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class WDHTClient():
    def __init__(self, ip='localhost', port=9090):
        self.ip = ip
        self.port = port

    def __getattr__(self,name):
        # Make socket
        transport = TSocket.TSocket(self.ip, self.port)
        # Buffering is critical. Raw sockets are very slow
        transport = TTransport.TBufferedTransport(transport)
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        # Create a client to use the protocol encoder
        client = Client(protocol)
        attr = getattr(client, name)
        if callable(attr):
            print "Hello World", attr
            def wrapped(*args, **kwargs):
                # Connect!
                transport.open()
                retval = attr(*args, **kwargs)
                transport.close()
                return retval
            return wrapped
        else:
            return attr

if __name__ == "__main__":
    C = WDHTClient()
    C.ping()
