#coding=utf8
import threading
import Queue
import argparse
from SimpleXMLRPCServer import SimpleXMLRPCServer
import time
import traceback
import subprocess

import common
from common import NodeInfo, RequestHandler, NodeStatus, Task, TaskResult
import utils
from log import logging
import conf

class HeartbeatThread(threading.Thread):
    '''用于与master保持心跳的专用线程'''
    def __init__(self, worker_node, heartbeat_duration=None):
        threading.Thread.__init__(self)

        self.worker_node = worker_node
        self.proxy = common.RPCServerProxy.get_proxy(worker_node.get_master_info())
        if heartbeat_duration is not None:
            self.heartbeat_duration = heartbeat_duration
        else:
            self.heartbeat_duration = conf.HEARTBEAT_DURATION

    def run(self):
        while True:
            try:
                time.sleep(self.heartbeat_duration)
                worker = self.worker_node.get_node_info()
                self.proxy.heartbeat(worker)
            except Exception,e:
                logging.warning(e.message)
                logging.warning(u"心跳失败")

class WorkerNode(object):
    '''作业节点，启动RPC线程后，先在master上注册，然后等待分配任务'''
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
        try:
            rt = self.proxy.register_worker(self.node_info)
        except Exception,e:
            logging.warning(e)
            logging.warning('''Server is not available!\nPlease check:\n1. Server is running.\n2. Network is ok''')
            exit(1)
        logging.info('register status: %s',rt)

    def assign_task(self, task):
        '''增加作业'''
        self.tasks.put(Task.from_dict(task))

    def is_idle(self):
        return self.tasks.qsize()==0 and not self.working

    def finish_task(self, task, result):
        self.working = False
        if self.is_idle():
            self.node_info.status = NodeStatus.idle
        else:
            self.node_info.status = NodeStatus.working

        self.node_info.update_heartbeat()
        try:
            self.proxy.task_complete(self.node_info, result)
        except Exception,e:
            logging.warning(e)

    def do_task(self):
        '''执行作业'''
        task = self.tasks.get()
        self.working = True
        # execute task here
        result = self._execute_task(task)
        # 告诉master作业完成
        self.finish_task(task, result)

    def _execute_task(self, task):
        logging.info(task)
        cmd = task.get_cmd()
        logging.info(cmd)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out,err = p.communicate()
        if p.returncode == 0:
            logging.info("success")
            logging.info(out)
        else:
            logging.info("failed")
        result = TaskResult(task.get_uuid(), p.returncode)
        return result

    def run(self):

        rpc = RPCWorkerThread(self)
        rpc.setDaemon(True)

        heartbeat_thread = HeartbeatThread(self)
        heartbeat_thread.setDaemon(True)

        rpc.start()

        self.register()

        heartbeat_thread.start()

        while True:
            try:
                self.do_task()
            except Exception,e:
                traceback.print_exc()
                logging.warning('task failed!')

class RPCWorkerThread(threading.Thread):
    '''负责提供给master调用的rpc接口的专用线程'''
    def __init__(self, worker_node):
        threading.Thread.__init__(self)
        self.worker_node = worker_node

        # Create server
        server = SimpleXMLRPCServer((worker_node.node_info.ip, worker_node.node_info.port),
                                    requestHandler=RequestHandler, logRequests=True)
        server.register_introspection_functions()
        server.register_function(self.assign_task)
        self.server = server

    def assign_task(self, task):
        '''master 给当前作业节点分配任务'''
        self.worker_node.assign_task(task)
        return True

    def run(self):
        self.server.serve_forever()

def get_worker(port):
    ip = utils.IPGetter.get_ip_address()
    master_info = NodeInfo(name="master",ip=conf.MASTER_IP,
                           port=conf.MASTER_PORT, status=NodeStatus.working)
    node_info = NodeInfo(name="worker", ip=ip, port=port, status=NodeStatus.idle)

    if common.doesServiceExist(node_info.ip, node_info.port):
        logging.warning("%s:%s already been used! change another port" % (node_info.ip, node_info.port))
        exit(1)

    worker_node = WorkerNode(master_info, node_info)
    logging.info("%s startup" % worker_node.get_node_info())

    return worker_node

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p","--port", help=u"作业节点rpc服务端口", type=int)
    args = parser.parse_args()
    port = conf.WORKER_PORT
    if args.port is not None:
        port = args.port

    worker_node = get_worker(port)

    worker_node.run()

if __name__ == '__main__':
    main()