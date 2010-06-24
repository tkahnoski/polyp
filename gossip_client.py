#!/usr/bin/env python


import optparse
import sys
sys.path.append('gen-py')

from polyp.gossip.ttypes import NewStateMessage
from polyp.ttypes import Endpoint
from polyp_client import PolypPool

def main(args):
  p = optparse.OptionParser("Add arbitraty state to a server")
  p.add_option("--server","-s",help="Server host (localhost)",
    default="localhost")
  p.add_option("--port","-p",help="Server port (9999)",
    type="int",default=9999)
  p.add_option("--key","-k",help="Key for state value to change (REQUIRED)",
    type="string",default=None)
  p.add_option("--value","-v",help="string value of state (REQUIRED)",
    type="string",default=None)
  opts,args = p.parse_args(args)
  if opts.key is None or opts.value is None:
    print "Please specify a state key  and value"
    p.print_help()
    sys.exit(1)
  msg = NewStateMessage(opts.key,opts.value)
  print 'Connecting to %s:%d' % (opts.server,opts.port)
  client_pool = PolypPool(Endpoint("client",0000))
  client = client_pool.get(Endpoint(opts.server,opts.port))
  client.send_obj("gossip","newstate",msg)
  print ""
  print "Message Sent"

if __name__ == '__main__':
  main(sys.argv[1:])
