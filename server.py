import threading
from SocketServer import ThreadingMixIn

from SimpleXMLRPCServer import SimpleXMLRPCServer

import conf
import master
from common import Task, TaskLoader, TaskResult
from common import NodeInfo, RequestHandler, doesServiceExist

class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer): pass

class RPCServerThread(threading.Thread):

    def __init__(self, master):
        threading.Thread.__init__(self)
        # Create server
        server = ThreadedXMLRPCServer((conf.MASTER_IP, conf.MASTER_PORT), requestHandler=RequestHandler, logRequests=True)
        server.register_introspection_functions()
        server.register_function(self.register_worker)
        server.register_function(self.logout_worker)
        server.register_function(self.task_complete)
        server.register_function(self.get_master_status)
        server.register_function(self.heartbeat)

        self.server = server
        self.master = master

    def register_worker(self, worker):
        nodeInfo = NodeInfo.from_dict(worker)
        return self.master.register_worker(nodeInfo)

    def logout_worker(self, worker):
        nodeInfo = NodeInfo.from_dict(worker)
        return self.master.remove_worker(nodeInfo)

    def task_complete(self, worker, taskResult):
        nodeInfo = NodeInfo.from_dict(worker)
        result = TaskResult.from_dict(taskResult)
        return self.master.task_complete(nodeInfo, result)

    def get_master_status(self):
        return self.master.get_status()

    def heartbeat(self, worker):
        nodeInfo = NodeInfo.from_dict(worker)
        return self.master.heartbeat(nodeInfo)

    def run(self):
        self.server.serve_forever()


def main():
    if doesServiceExist(conf.MASTER_IP, conf.MASTER_PORT):
        print("%s:%s already been used! change another port" % (conf.MASTER_IP, conf.MASTER_PORT))
        exit(1)
    master_node = master.Master(TaskLoader(), conf)
    server = RPCServerThread(master_node)
    server.start()
    master_node.serve_forever()

if __name__ == '__main__':
    main()

