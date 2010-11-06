#!/usr/bin/env python

import polyp_client
import polyp_server
import sys
import optparse
import polyp_util

from polyp.ttypes import Endpoint
from polyp.gossip.ttypes import GetStateMessage

class Client(object):
	def __init__(self, address, port, known_polyps=[]):
		self.known = known_polyps
		self.endpoint = Endpoint(address, port)
		self.messager = polyp_server.MessageHandler()
		self.server = polyp_server.PolypServer(address, port,
												self.messager,
												seeds=[],
												background=True,
												server=False)
		self.polyp_pool = polyp_client.PolypPool(self.endpoint)
	def add_polyp(self,args):
		host, port = args.strip().split(':')
		if port == "":
			return "Expected <host>:<port> as arguments"
		self.known.append((host,int(port)))
	def show_polyps(self, args):
		for i, pair in enumerate(self.known):
			print i, ': ', pair
	def exec_cmd(self,cmd,args):
		if len(cmd) == 0:
			return
		try:
			hasattr(self,cmd)
			method = getattr(self,cmd)
			ret = method(args)
			if ret:
				print "Error in ", cmd, ": ", ret
		except (NameError), e:
			print "Method: '", cmd, "' not found"
			print e
	def get_state(self,args):
		if len(args) == 0:
			return "Expected: <polyp index> as arguments"
		idx = int(args.strip())
		if idx >= len(self.known):
			self.show_polyps("")
			return "idx is out of range"
		msg = GetStateMessage(
				[Endpoint(host, port) for host,port in self.known])
		client = self._get_remote_client(self.known[idx][0],self.known[idx][1])
		client.send_obj("gossip","getstate",msg)
	def _get_remote_client(self,host,port):
		return self.polyp_pool.get(Endpoint(host,port))
	def help(self,args):
		print "add_polyp <host>:<port> -- Add a polyp to the index"
		print "get_state <polyp index> -- retrieve Polyp State information"
		print "help -- display this message"
		print "quit -- Exit the prompt"
		print "show_polyps -- Show the polyps and their index"
	def loop(self,input=sys.stdin):
		self.help("")
		sys.stdout.write('> ')
		sys.stdout.flush()
		line = input.readline()
		while line and len(line) > 0:
			cmd_and_args = line.strip().split(" ",1)
			cmd = cmd_and_args[0].strip()
			if len(cmd_and_args) == 1:
				args = ""
			else:
				args = cmd_and_args[1].strip()
			if cmd == "quit":
				return
			self.exec_cmd(cmd,args)
			sys.stdout.write('> ')
			sys.stdout.flush()
			line = input.readline()
			

def main(args):
	p = optparse.OptionParser()
	p.add_option("--gossip","-g",default=None,help="List of polyp host ports")
	p.add_option("--address","-a",default="localhost",
		help="Address for servers to send response too")
	p.add_option("--port","-p",default="9000", type="int",
		help="Port for server to send responses too")

	opts,args = p.parse_args(args)

	client = Client(opts.address,opts.port)
	hosts = []
	if opts.gossip:
		handle = open(opts.gossip,'r')
		for line in handle:
			client.add_polyp(line)
		handle.close()
	try:
		client.loop()
		sys.exit(0)
	except KeyboardInterrupt, e:
		sys.exit(0)
	except Exception as e:
		print e
		sys.exit(-1)
if __name__ == '__main__':
	main(sys.argv[1:])
