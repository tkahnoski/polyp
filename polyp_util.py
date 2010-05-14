#!/usr/bin/env python

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

def serialize(thrift_obj):
  transportOut = TTransport.TMemoryBuffer()
  protocolOut = TBinaryProtocol.TBinaryProtocol(transportOut)
  thrift_obj.write(protocolOut)
  return transportOut.getvalue()

def deserialize(thrift_obj,binary):
  transportIn = TTransport.TMemoryBuffer(binary)
  protocolIn = TBinaryProtocol.TBinaryProtocol(transportIn)
  thrift_obj.read(protocolIn)
  return thrift_obj

