#!/usr/bin/env python

import sys
from threading import Thread
import random
import time
sys.path.append('./gen-py')

import polyp_util
from polyp.ttypes import Endpoint
from polyp.gossip.ttypes import GossipSynMessage, GossipAckMessage, GossipAckBackMessage
from polyp.gossip.constants import FIRST_GEN
from polyp.gossip.ttypes import HeartBeatState, EndpointState, EndpointDigest
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport


VERB_SYN="syn"
VERB_ACK="ack"
VERB_ACKBACK="ackback"
#TODO: delete
def debug_print(header,obj):
  print "-" * 80
  print header
  print obj

#parse host:port lines into (host,port)
def read_seeds(stream):
  seeds = []
  for line in stream:
    comments = line.split('#',1)
    stripped = comments[0].strip()
    if len(stripped) > 0:
      cols = stripped.split(':',1)
      seeds.append((cols[0],int(cols[1])) )
  return seeds

class SynThread(Thread):
  def __init__(self,gossiper,delay):
    Thread.__init__(self,name="SynThread")
    self.daemon = True
    self.gossiper = gossiper
    self.delay = delay
    self.start()
  def run(self):
    while True:
      time.sleep(self.delay)
      self.gossiper.gossip_round()

def ep_pair(endpoint):
 return (endpoint.address,endpoint.port)

class GossipHandler:
  #Initialize gossiper and start thread to periodically gossip
  #Requires generation number
  def __init__(self,sending_service,local,generation,seeds=[],delay=4.0):
    self.generation = generation
    self.local = local
    self.heartbeat = HeartBeatState(generation,1)
    self.state = EndpointState(self.local,self.heartbeat,{})
    self.endpoints = {}
    self.endpoints[ep_pair(self.local)] = self.state
    for seed in seeds:
      self.endpoints[seed] = \
        EndpointState(Endpoint(seed[0],seed[1]),HeartBeatState(FIRST_GEN,0),{})
    self.living = []
    self.outbound = sending_service
    self.synThread = SynThread(self,delay)
  def syn(self,sender,syn_msg):
    debug_print('syn:', syn_msg)
    unknown, known = examine_digest_list(syn_msg.digests,self.endpoints)
    ackMsg = GossipAckMessage(unknown,known)
    self.outbound.get(sender).send_obj("gossip",VERB_ACK,ackMsg)
  def ack(self,sender,ack_msg):
    debug_print('ack:', ack_msg)
    self.update_states(ack_msg.known)
    if len(ack_msg.requested) > 0:
      unknown, known = examine_digest_list(ack_msg.requested,self.endpoints)
      ackBackMsg = GossipAckBackMessage(known)
      self.outbound.get(sender).send_obj("gossip",VERB_ACKBACK,ackBackMsg)
  def ackBack(self,sender,ack_back_msg):
    debug_print('ack_back:', ack_back_msg)
    self.update_states(ack_back_msg.known)
  #Take a polyp Message and dispatch based on verb
  def handleMessage(self,message):
    #TODO: this needs to be fast
    if message.header.verb == VERB_SYN:
      synMsg = polyp_util.deserialize(GossipSynMessage(),message.body)
      self.syn(message.header.sender,synMsg)
    elif message.header.verb == VERB_ACK:
      ackMsg = polyp_util.deserialize(GossipAckMessage(),message.body)
      self.ack(message.header.sender,ackMsg)
    elif message.header.verb == VERB_ACKBACK:
      ackBackMsg = polyp_util.deserialize(
          GossipAckBackMessage(),message.body)
      self.ackBack(message.header.sender,ackBackMsg)
  def send_random(self,endpoints,message,msg_type,msg_verb):
    if len(endpoints) == 1:
      return
    chosen = random.choice(endpoints)
    local_key = ep_pair(self.local)
    if chosen != local_key:
      eps = endpoints[:]
      eps.remove(local_key)
      chosen = random.choice(eps)
    print 'sending syn to ', chosen
    contact = Endpoint(chosen[0],chosen[1])
    self.outbound.get(contact).send_obj(msg_type,msg_verb,message)
  def gossip_round(self):
    self.heartbeat.version += 1
    print "-" * 80
    print 'Starting gossip round %d ' % self.heartbeat.version
    if len(self.endpoints) == 1:
      print 'No one to gossip to :-('
      return
    digests = self.make_random_digests()
    synMsg = GossipSynMessage(digests)
    self.send_random(self.endpoints.keys(),synMsg,"gossip",VERB_SYN)
  def make_random_digests(self):
    #Assume we have lockk
    sample_size = min(len(self.endpoints),31)
    rand_endpoints = random.sample(self.endpoints.values(),sample_size)
    if not self.state in rand_endpoints:
      rand_endpoints.append(self.state)
    return [EndpointDigest(x.endpoint,
              max_generation(x),
              max_version(x))
            for x in rand_endpoints]
  def update_states(self,epStates):
    for state in epStates:
      if state.endpoint == self.local:
        continue
      ep_key = ep_pair(state.endpoint)
      local_ep = self.endpoints.get(ep_key)
      if local_ep is None:
        #TODO
        # check quarantine
        # notifications?
        self.endpoints[ep_key] = state
      elif local_ep.heartbeat.generation < state.heartbeat.generation:
        self.endpoints[ep_key] = state
        #TODO: notifications?
      elif local_ep.heartbeat.generation == state.heartbeat.generation \
        and max_version(local_ep) < max_version(state):
        update_ep_state(local_ep,state)

def max_version(state):
  states = state.info.values()[:]
  states.append(state.heartbeat)
  return max(states,key=lambda x: x.version).version
def max_generation(state):
  states = state.info.values()[:]
  states.append(state.heartbeat)
  return max(states,key=lambda x: x.generation).generation
#Return a new endpoint state
def get_bigger_states(epState,version):
  states = filter(lambda x: x[1].version > version, epState.info.items())
  return EndpointState(epState.endpoint, epState.heartbeat,dict(states))

#Return tuple of states that need updating and known states
def examine_digest_list(remote_digests,localEpMap):
  unknown = []
  knownStates = []
  for digest in remote_digests:
    remote_gen = digest.generation
    remote_v = digest.version
    ep_key = (digest.endpoint.address,digest.endpoint.port)
    epState = localEpMap.get(ep_key)
    if not epState is None:
      local_gen = max_generation(epState)
      local_v = max_version(epState)
      if remote_gen == local_gen and remote_v == local_v:
        continue
      if remote_gen > local_gen:
        unknown.append(digest.endpoint,remote_gen,0)
      elif remote_gen < local_gen:
        knownStates.append(get_bigger_states(epState,0))
      elif remote_v > local_v:
        unknown.append(
          EndpointDigest(digest.endpoint,remote_gen,local_v))
      elif remote_v < local_v:
        knownStates.append(get_bigger_states(epState,remote_v))
    else:
      unknown.append(EndpointDigest(digest.endpoint,remote_gen,0))
  return (unknown,knownStates)

#Mutate current state in place with changes from latest
def update_ep_state(current_state,latest_state):
 current_state.heartbeat = latest_state.heartbeat
 current_map = current_state.info
 latest_map = latest_state.info
 for k, latest_v in latest_map.items():
   current_v = current_map.get(k)
   if current_v is None:
     current_map[k] = latest_v
   elif current_v.generation < latest_v.generation:
     current_map[k] = latest_v
   elif current_v.generation == latest_v.generation \
     and current_v.version < latest_v.version:
     current_map[k] = latest_v
 
