#coding=utf8
import xmlrpclib
import threading
import Queue
import argparse
from SimpleXMLRPCServer import SimpleXMLRPCServer
import time
import traceback

import common
from common import NodeInfo, RequestHandler
import conf_master
import conf_client

class HeartbeatThread(threading.Thread):
    '''用于与master保持心跳的专用线程'''
    def __init__(self, worker_node, heartbeat_duration=None):
        threading.Thread.__init__(self)

        self.worker_node = worker_node
        self.proxy = common.RPCServerProxy.get_proxy(worker_node.get_master_info())
        if heartbeat_duration is not None:
            self.heartbeat_duration = heartbeat_duration
        else:
            self.heartbeat_duration = conf_master.HEARTBEAT_DURATION

    def run(self):
        while True:
            try:
                time.sleep(self.heartbeat_duration)
                self.proxy.heartbeat(self.worker_node.get_node_info())
            except Exception,e:
                traceback.print_exc()

class WorkerNode(object):
    '''作业节点，启动后先在master上注册，然后等待分配任务'''
    def __init__(self, master_info, node_info):
        self.proxy = common.RPCServerProxy.get_proxy(master_info)
        self.tasks = Queue.Queue()
        self.node_info = node_info
        self.master_info = master_info

    def get_node_info(self):
        return self.node_info

    def get_master_info(self):
        return self.master_info

    def register(self):
        '''告诉master'''
        rt = self.proxy.register_worker(self.node_info)
        print('register status',rt)

    def assign_task(self, task):
        '''增加作业'''
        self.tasks.put(task)

    def do_task(self):
        '''执行作业'''
        task = self.tasks.get()
        # do task here
        print(task)
        time.sleep(3)
        # 告诉master作业完成
        self.proxy.task_complete(self.node_info, task, {})

    def run(self):
        while True:
            try:
                self.do_task()
            except Exception,e:
                traceback.print_exc()

class RPCWorkerThread(threading.Thread):
    '''负责提供给master调用的rpc接口的专用线程'''
    def __init__(self, worker_node):
        threading.Thread.__init__(self)
        self.worker_node = worker_node

        # Create server
        server = SimpleXMLRPCServer((worker_node.node_info.ip, worker_node.node_info.port), requestHandler=RequestHandler, logRequests=True)
        server.register_introspection_functions()
        server.register_function(self.assign_task)
        self.server = server

    def assign_task(self, task):
        '''master 给当前作业节点分配任务'''
        self.worker_node.assign_task(task)
        return True

    def run(self):
        self.server.serve_forever()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p","--port", help=u"作业节点rpc服务端口", type=int)
    args = parser.parse_args()

    master_info = NodeInfo(name="master",ip=conf_master.MASTER_IP,port=conf_master.MASTER_PORT, status='ready')
    node_info = NodeInfo(name="worker", ip=conf_client.WORKER_IP, port=conf_client.WORKER_PORT, status='ready')

    if args.port:
        node_info.port=args.port

    if common.doesServiceExist(node_info.ip, node_info.port):
        print("%s:%s already been used! change another port" % (node_info.ip, node_info.port))
        exit(1)
    
    worker_node = WorkerNode(master_info, node_info)
    
    print(worker_node.get_node_info())

    prc = RPCWorkerThread(worker_node)
    heartbeat_thread = HeartbeatThread(worker_node)

    prc.start()
    heartbeat_thread.start()

    worker_node.register()
    worker_node.run()

if __name__ == '__main__':
    main()