#!/usr/bin/env python

import optparse
import socket
import sys
import threading
import time
sys.path.append('./gen-py')

from polyp import Daemon
from polyp.ttypes import Header, Endpoint


from polyp_client import PolypPool
import polyp_util
from gossip_handler import GossipHandler, read_seeds

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
    #print "Polyp Handler initialized"
    self.handlers = handlers
  def receive(self,message):
    #print "Received from %s %s" % (message.header.sender, message.header.id)
    #print message.header.msg_type
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
  p.add_option("-b","--background",help="Do not take signals from stdin",
    action="store_true")
  p.add_option("-g","--gossip", default=None,
    help="Gossip seed file. line seperated host:port")
  opts,args = p.parse_args(args)
  seeds = []
  if opts.gossip:
    seed_file = open(opts.gossip)
    seeds = read_seeds(seed_file)
    seed_file.close()
  address = opts.address
  port = opts.port
  background = opts.background
  messager = MessageHandler()
  PolypServer(address, port,
              messager,
              seeds=seeds,
              background=opts.background)

class PolypServer(object):
  def __init__(self, address, port, messager,
  				seeds=[],
				background=False,
				server=True):
    endpoint = Endpoint(address,port)
    gossiper = GossipHandler(PolypPool(endpoint),
					endpoint,
					0,
					seeds,
					auto_synch=server)
    messager.registerHandler('gossip',gossiper)
    messager.registerHandler('echo',EchoHandler)
    processor = Daemon.Processor(messager)
    transport = MyTServerSocket(address,port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)
    def server_thread():
      try:
        server.serve()
      except KeyboardInterrupt, e:
        print "Exiting..."
        sys.exit(0)
    try:
       thread = threading.Thread(target=server_thread,name="ServerThread")
       print "Starting polyp server en %s:%d" % (address,port)
       if not background:
          print "Ctrl^D or 'exit' to quit"
       thread.start()
       if not background:
          for line in sys.stdin:
             if line.strip() == 'exit':
              break
          print "Exiting..."
          sys.exit(0)
    except KeyboardInterrupt, e:
       print "Exiting..."
       sys.exit(0)

if __name__ == '__main__':
  main(sys.argv[1:])
