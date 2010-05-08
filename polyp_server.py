#!/usr/bin/env python

import sys

sys.path.append('./gen-py')

from polyp import Daemon

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

class MessageHandler:
  def __init__(self):
    print "Handler initialized"
  def receive(self,message):
    print "Received: %s" % message.header.id
    print message


def main(args):
  if len(args) > 1:
    print "usage: polyp_server.py [port | 9999]"
    sys.exit(1)
  port = 9999
  if len(args) > 0:
    port = int(args[1])
  handler = MessageHandler()
  processor = Daemon.Processor(handler)
  transport = TSocket.TServerSocket(port)
  tfactory = TTransport.TBufferedTransportFactory()
  pfactory = TBinaryProtocol.TBinaryProtocolFactory()

  server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)


  print "Starting polyp server on %d" % port
  server.serve()

if __name__ == '__main__':
  main(sys.argv[1:])
