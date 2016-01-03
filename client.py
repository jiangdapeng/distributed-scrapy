import xmlrpclib
import threading
import Queue

from SimpleXMLRPCServer import SimpleXMLRPCServer

import common
from common import NodeInfo, RequestHandler

class WorkerNode(object):

	def __init__(self, master_info):
		self.proxy = common.RPCServerProxy.get_proxy(master_info)
		self.tasks = Queue.Queue()

	def register(self):
		rt = self.proxy.register_worker({
			'name': 'test',
			'ip': 'localhost',
			'port': 8001,
			'status': 'ready'
			})

	def assign_task(self, task):
		self.tasks.put(task)

	def do_task(self):
		task = self.tasks.get()
		print(task)

	def run(self):
		while True:
			self.do_task()

class RPCWorkerThread(threading.Thread):

	def __init__(self, worker_node):
		threading.Thread.__init__(self)
		self.worker_node = worker_node

		# Create server
		server = SimpleXMLRPCServer(("localhost", 8001), requestHandler=RequestHandler, logRequests=True)
		server.register_introspection_functions()
		server.register_function(self.assign_task)
		self.server = server

	def assign_task(self, task):
		self.worker_node.assign_task(task)
		return True

	def run(self):
		self.server.serve_forever()

def main():
	master_info = NodeInfo(name="master",ip='localhost',port=8000, status='ready')
	worker_node = WorkerNode(master_info)
	
	prc = RPCWorkerThread(worker_node)
	prc.start()

	worker_node.register()
	worker_node.run()

if __name__ == '__main__':
	main()