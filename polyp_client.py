#!/usr/bin/env python


import optparse
import sys
sys.path.append('gen-py')

from polyp import Daemon
from polyp.ttypes import Header, Message

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

def main(args):
  p = optparse.OptionParser()
  p.add_option("--server","-s",help="Server host",default="localhost")
  p.add_option("--port","-p",help="Server port",type="int",default=9999)
  
  opts,args = p.parse_args(args)
  messages = args
  print 'Connecting to %s:%d' % (opts.server,opts.port)
  transport = TSocket.TSocket(opts.server,opts.port)
  transport = TTransport.TBufferedTransport(transport)
  protocol = TBinaryProtocol.TBinaryProtocol(transport)

  client = Daemon.Client(protocol)
  transport.open()
  
  if len(messages) == 0:
    print 'Reading messages from stdin one per line'
    messages = sys.stdin
  cnt = 0
  for line in messages:
    head = Header(id=str(cnt),verb=line)
    msg = Message(head)
    client.receive(msg)
    cnt += 1
    sys.stdout.write('.')
    if cnt % 50 == 0:
      sys.stdout.write("%d\n" % cnt)
if __name__ == '__main__':
  main(sys.argv[1:])
