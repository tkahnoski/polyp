#!/usr/bin/env python


import optparse
import sys
sys.path.append('../gen-py')

from polyp import Daemon

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.transport import TBinaryProtocol


def main(args):
  p = optparse.OptionParser()
  p.add_option("s","server",help="Server host",type="string")
  p.add_option("p","port",help="Server port",type="int")
  
  opts,args = p.parse_args(args)
  messages = args
  if len(messages) == 0:
    print 'Reading messages from stdin one per line'
    messages = sys.stdin

