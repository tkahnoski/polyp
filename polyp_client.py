#!/usr/bin/env python


import optparse
import sys
sys.path.append('gen-py')

from polyp import Daemon
from polyp.ttypes import Endpoint, Header, Message
import polyp_util

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class PolypClient(object):
  def __init__(self,this_endpoint,remote):
    self.msg_id = 0
    self.endpoint = this_endpoint
    self.remote = remote
    self.transport = \
        TSocket.TSocket(self.remote.address,self.remote.port)
    self.buf_transport = TTransport.TBufferedTransport(self.transport)
    self.protocol = TBinaryProtocol.TBinaryProtocol(self.buf_transport)
    self.client = Daemon.Client(self.protocol)
    self.buf_transport.open()
  def send_obj(self,msg_type,msg_verb,msg_object):
    print msg_object
    msg_body = polyp_util.serialize(msg_object)
    header = Header(self.getMsgId(),self.endpoint,msg_type,msg_verb)
    msg = Message(header,msg_body)
    self.send_msg(msg)
  def send_msg(self,msg):
    self.client.receive(msg)
  def getMsgId(self):
    id = self.msg_id
    self.msg_id += 1
    return str(id)

class PolypPool:
#Thrift objects aren't hashable so we use key (address,port)
  MAX_POOLSIZE=4
  def __init__(self,this_endpoint):
    self.endpoint = this_endpoint
    self.client_pool = {}
  def get(self,remote):
    #TODO: lock
    key = (remote.address,remote.port)
    items = self.client_pool.get(key,None)
    if items is None:
      items = []
      self.client_pool[key] = items
    if len(items) == 0:
      return PolypClient(self.endpoint,remote)
    client = items.pop()
    self.client_pool[key] = items
    return client
  def release_client(self,client):
    #TODO: lock
    #TODO: put LRU remote if MAX_ENDPOINTS is exceeded
    key = (client.remote.address,client.remote.port)
    self.client_pool.setdefault(key,[])
    if len(self.client_pool[key]) >= PolypPool.MAX_POOLSIZE:
      return
    self.client_pool[key].append(client)



def main(args):
  p = optparse.OptionParser()
  p.add_option("--server","-s",help="Server host",default="localhost")
  p.add_option("--port","-p",help="Server port",type="int",default=9999)

  opts,args = p.parse_args(args)
  messages = args
  print 'Connecting to %s:%d' % (opts.server,opts.port)
  client_pool = PolypPool(Endpoint("localhost",0000))
  client = client_pool.get(Endpoint(opts.server,opts.port))
  if len(messages) == 0:
    print 'Reading messages from stdin one per line'
    messages = sys.stdin
  cnt = 0
  for line in messages:
    dumbHeader = Header(str(cnt),Endpoint("",0000),"",line.strip())
    client.send_obj("echo","",dumbHeader)
    cnt += 1
    sys.stdout.write('.')
    if cnt % 50 == 0:
      sys.stdout.write("%d\n" % cnt)
  print ""
  print "Sent %d messages" % cnt

if __name__ == '__main__':
  main(sys.argv[1:])
