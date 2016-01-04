import threading
from SocketServer import ThreadingMixIn

from SimpleXMLRPCServer import SimpleXMLRPCServer

import conf_master
import master
from master import Task, TaskLoader
from common import NodeInfo, RequestHandler

class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer): pass

class RPCServerThread(threading.Thread):

	def __init__(self, master):
		threading.Thread.__init__(self)
		# Create server
		server = ThreadedXMLRPCServer((conf_master.MASTER_IP, conf_master.MASTER_PORT), requestHandler=RequestHandler, logRequests=True)
		server.register_introspection_functions()
		server.register_function(self.register_worker)
		server.register_function(self.logout_worker)
		server.register_function(self.task_complete)
		server.register_function(self.get_master_status)
		server.register_function(self.heartbeat)

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

	def task_complete(self, worker, task, stats):
		nodeInfo = NodeInfo(
			name = worker['name'],
			ip = worker['ip'],
			port = worker['port'],
			status =worker['status']
			)
		taskObj = Task(task['identifier'], task['project'], task['spider_name'], task['urls'])
		return self.master.task_complete(nodeInfo, taskObj, stats)

	def get_master_status(self):
		return self.master.get_status()

	def heartbeat(self, worker):
		nodeInfo = NodeInfo(
			name = worker['name'],
			ip = worker['ip'],
			port = worker['port'],
			status =worker['status']
			)
		return self.master.heartbeat(nodeInfo)

	def run(self):
		self.server.serve_forever()


def main():
	master_node = master.Master(TaskLoader(), conf_master)
	server = RPCServerThread(master_node)
	server.start()
	master_node.serve_forever()

if __name__ == '__main__':
	main()

