import threading

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler

import master
from master import TaskLoader
from common import NodeInfo, RequestHandler


class RPCServerThread(threading.Thread):

	def __init__(self, master):
		threading.Thread.__init__(self)
		# Create server
		server = SimpleXMLRPCServer(("localhost", 8000), requestHandler=RequestHandler, logRequests=True)
		server.register_introspection_functions()
		server.register_function(self.register_worker)
		server.register_function(self.logout_worker)

		self.server = server
		self.master = master

	def register_worker(self, worker):
		nodeInfo = NodeInfo(
			name = worker['name'],
			ip = worker['ip'],
			port = worker['port'],
			status =worker['status']
			)
		return self.master.register_worker(nodeInfo)

	def logout_worker(self, worker):
		nodeInfo = NodeInfo(
			name = worker['name'],
			ip = worker['ip'],
			port = worker['port'],
			status =worker['status']
			)
		return self.master.remove_worker(nodeInfo)

	def run(self):
		self.server.serve_forever()


def main():
	master_node = master.Master(TaskLoader())
	server = RPCServerThread(master_node)
	server.start()
	master_node.serve_forever()

if __name__ == '__main__':
	main()

