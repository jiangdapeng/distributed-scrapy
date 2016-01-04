#coding=utf8
import xmlrpclib
import threading
import Queue
import argparse
from SimpleXMLRPCServer import SimpleXMLRPCServer
import time
import common
from common import NodeInfo, RequestHandler

class WorkerNode(object):

	def __init__(self, master_info, node_info):
		self.proxy = common.RPCServerProxy.get_proxy(master_info)
		self.tasks = Queue.Queue()
		self.node_info = node_info

	def register(self):
		rt = self.proxy.register_worker(self.node_info)
		print('register status',rt)

	def assign_task(self, task):
		self.tasks.put(task)

	def do_task(self):
		task = self.tasks.get()
		# do task here
		print(task)
		time.sleep(3)
		# tell master stask done
		self.proxy.task_complete(self.node_info, task, {})

	def run(self):
		while True:
			self.do_task()

class RPCWorkerThread(threading.Thread):

	def __init__(self, worker_node):
		threading.Thread.__init__(self)
		self.worker_node = worker_node

		# Create server
		server = SimpleXMLRPCServer((worker_node.node_info.ip, worker_node.node_info.port), requestHandler=RequestHandler, logRequests=True)
		server.register_introspection_functions()
		server.register_function(self.assign_task)
		self.server = server

	def assign_task(self, task):
		self.worker_node.assign_task(task)
		return True

	def run(self):
		self.server.serve_forever()

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-p","--port", help=u"作业节点rpc服务端口", type=int)
	args = parser.parse_args()

	master_info = NodeInfo(name="master",ip='localhost',port=8000, status='ready')
	if args.port:
		node_info = NodeInfo(name="worker", ip='localhost', port=args.port, status='ready')
	else:
		node_info = NodeInfo(name="worker", ip='localhost', port=8001, status='ready')
	worker_node = WorkerNode(master_info, node_info)
	
	prc = RPCWorkerThread(worker_node)
	prc.start()

	worker_node.register()
	worker_node.run()

if __name__ == '__main__':
	main()