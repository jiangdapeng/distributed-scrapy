#coding=utf8
import xmlrpclib
import threading
import Queue
import argparse
from SimpleXMLRPCServer import SimpleXMLRPCServer
import time
import traceback

import common
from common import NodeInfo, RequestHandler, NodeStatus
import utils
from log import logging
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
                worker = self.worker_node.get_node_info()
                self.proxy.heartbeat(worker)
            except Exception,e:
                traceback.print_exc()

class WorkerNode(object):
    '''作业节点，启动后先在master上注册，然后等待分配任务'''
    def __init__(self, master_info, node_info):
        self.proxy = common.RPCServerProxy.get_proxy(master_info)
        self.tasks = Queue.Queue()
        self.node_info = node_info
        self.master_info = master_info
        self.working = False

    def get_node_info(self):
        if self.is_idle():
            self.node_info.status = NodeStatus.idle
        else:
            self.node_info.status = NodeStatus.working
        self.node_info.update_heartbeat()
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

    def is_idle(self):
        return self.tasks.qsize()==0 and not self.working

    def do_task(self):
        '''执行作业'''
        task = self.tasks.get()
        self.working = True
        # do task here
        print(task)
        time.sleep(3)
        # 告诉master作业完成
        self.working = False
        self.node_info.update_heartbeat()
        self.proxy.task_complete(self.node_info, task, {})

    def run(self):

        prc = RPCWorkerThread(self)
        prc.setDaemon(True)

        heartbeat_thread = HeartbeatThread(self)
        heartbeat_thread.setDaemon(True)

        prc.start()

        try:
            self.register()
        except Exception,e:
            # traceback.print_exc()
            logging.warning('''Server is not available!\nPlease check: \n1. Server is running.\n 2. Network is ok''')
            exit(1)

        heartbeat_thread.start()

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

def get_worker(ip, port):
    master_info = NodeInfo(name="master",ip=conf_master.MASTER_IP,port=conf_master.MASTER_PORT, status=NodeStatus.working)
    node_info = NodeInfo(name="worker", ip=ip, port=port, status=NodeStatus.idle)

    if common.doesServiceExist(node_info.ip, node_info.port):
        print("%s:%s already been used! change another port" % (node_info.ip, node_info.port))
        exit(1)

    worker_node = WorkerNode(master_info, node_info)
    print(worker_node.get_node_info())

    return worker_node

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p","--port", help=u"作业节点rpc服务端口", type=int)
    args = parser.parse_args()
    ip = utils.IPGetter.get_ip_address()
    port = conf_client.WORKER_PORT
    if args.port is not None:
        port = args.port

    worker_node = get_worker(ip, port)

    worker_node.run()

if __name__ == '__main__':
    main()