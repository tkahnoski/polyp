#!/usr/bin/env python

import optparse
import socket
import sys
sys.path.append('./gen-py')

from polyp import Daemon
from polyp.ttypes import Header, Endpoint

import polyp_util

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

class MyTServerSocket(TSocket.TServerSocket):
  def __init__(self,host,port):
    TSocket.TServerSocket.__init__(self,port)
    self.host = host

class MessageHandler:
  def __init__(self, handlers={}):
    print "Polyp Handler initialized"
    self.handlers = handlers
    self._endpoint = None
  def receive(self,message):
    print "Received from %s %s" % (message.header.sender, message.header.id)
    print message.header.msg_type
    for handler in self.handlers.get(message.header.msg_type,[]):
      handler.handleMessage(message)
  def registerHandler(self,msg_type,handler):
    if not msg_type in self.handlers:
      self.handlers[msg_type] = []
    self.handlers[msg_type].append(handler)

class EchoHandler:
  def __init__(self):
    pass
  def handleMessage(self,message):
    echoHeader = polyp_util.deserialize(Header(),message.body)
    print "Echo: ", echoHeader.verb 

def main(args):
  p = optparse.OptionParser("Start the polyp server")
  p.add_option("-p","--port",help="Port to listen on",
    default=9999,type="int")
  p.add_option("-a","--address",help="Address this server is on",
    default="localhost")
  opts,args = p.parse_args(args)

  address = opts.address
  port = opts.port
  handlers = {'echo':[EchoHandler()]}
  start_polyp_server(address,port,handlers)

def start_polyp_server(address,port,handlers):
  handler = MessageHandler(handlers)
  processor = Daemon.Processor(handler)
  transport = MyTServerSocket(address,port)
  tfactory = TTransport.TBufferedTransportFactory()
  pfactory = TBinaryProtocol.TBinaryProtocolFactory()

  server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)
  endpoint = Endpoint(address,port)
  print "Starting polyp server en %s:%d" % (endpoint.address,endpoint.port)
  try:
    server.serve()
  except KeyboardInterrupt, e:
    print "Exiting..."
    sys.exit(0)

if __name__ == '__main__':
  main(sys.argv[1:])
