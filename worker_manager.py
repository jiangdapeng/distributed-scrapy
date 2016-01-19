# -*-coding=utf-8-*-
__author__ = 'jiangdapng(asuwill.jdp@gmail.com)'

import Queue
import threading

import common
from common import NodeStatus

__all__ = ['WorkerManager']

class WorkerManager(object):

    def __init__(self, conf):
        self.conf = conf
        self.idleWorkersQueue = Queue.Queue()
        self.workers = {} # 所有登记过的worker
        self.workersInQueue = {} # 记录所有在队列中的worker
        self.lock = threading.Lock()

    def next_worker(self):
        '''返回一个空闲的worker，如果没有会阻塞程序执行'''
        while True:
            worker = self.idleWorkersQueue.get()
            if worker.get_identifier() in self.workers:
                worker.status = NodeStatus.working
                self.update_worker(worker)
                break
        return worker

    def add_worker(self, worker):
        '''新增一个worker'''
        worker.status = NodeStatus.idle
        self.update_worker(worker)

    def remove_worker(self, worker_id):
        '''移除worker'''
        self.lock.acquire()
        self.workersInQueue.pop(worker_id, None)
        worker = self.workers.pop(worker_id, None)
        self.lock.release()
        return worker

    def update_worker(self, worker):
        '''更新worker信息'''
        self.lock.acquire()
        id = worker.get_identifier()
        status = worker.status
        self.workers[id] = worker
        if status == NodeStatus.idle and id not in self.workersInQueue:
            self.workersInQueue[id] = worker
            self.idleWorkersQueue.put(worker)
        elif status == NodeStatus.working:
            if id in self.workersInQueue:
                self.workersInQueue.pop(id, None)
        self.lock.release()

    def query_worker(self, worker_id):
        '''查询worker信息'''
        return self.workers.get(worker_id, None)


    def get_workers(self):
        return self.workers.copy()

    def get_idle_workers(self):
        return self.workersInQueue.copy()

    def _is_die(self, worker):
        timestamp = common.get_timestamp()
        return timestamp - worker.get_heartbeat() > self.conf.DIE_THRESHOLD

    def clean_death_workers(self):
        '''定期检查worker的心跳信息，及时清除死亡worker'''
        self.lock.acquire()

        died_workers = set()
        for worker_id,worker in self.workers.items():
            if self._is_die(worker):
                died_workers.add(worker_id)
        for worker_id in died_workers:
            self.workers.pop(worker_id, None)
            self.workersInQueue.pop(worker_id, None)

        self.lock.release()
        return died_workers

