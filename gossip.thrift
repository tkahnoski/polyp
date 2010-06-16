#
# Gossip messages and state.
# These are mostly based off of Cassandra's implementaiton
# http://wiki.apache.org/cassandra/ArchitectureGossip
# which is based on the paper
# http://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf
#

namespace cpp polyp.gossip
namespace java polyp.gossip
namespace py polyp.gossip

include "polyp.thrift"

struct HeartBeatState
{
  1: required i64 generation
  2: required i64 version
}

const i64 FIRST_GEN = 0;

struct ApplicationState
{
  1: required string key
  2: required string value
  3: required i64 generation
  4: required i64 version
}

struct EndpointState
{
  1: required polyp.Endpoint endpoint
  2: required HeartBeatState heartbeat
  3: required map<string,ApplicationState> info
}

struct EndpointDigest
{
  1: polyp.Endpoint endpoint
  2: i64 generation
  3: i64 version
}
#  Massive hand waving over the actual algorithm:
#  Gossiper A sends SynMessage to Gossiper B with it's known states
#  B sends A the states B has a higher version for and requests from A 
#    the states it has a lower version for
#  A then send B back the versions B requested

struct GossipSynMessage
{
  1: required list<EndpointDigest> digests
}

struct GossipAckMessage
{
  1: required map<string,HeartBeatState> requested
  2: required list<EndpointState> known
}

struct GossipAckBackMessage
{
  1: required list<EndpointState> known
}

service Gossiper
{
  void syn(1: polyp.Endpoint sender, 2: GossipSynMessage message);
  void ack(1: polyp.Endpoint sender, 2: GossipAckMessage message);
  void ackBack(1: polyp.Endpoint sender, 2: GossipAckBackMessage message);
}

