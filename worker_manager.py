# -*-coding=utf-8-*-
__author__ = 'jiangdapng(asuwill.jdp@gmail.com)'

import Queue
import threading

import common
from common import NodeStatus
from log import logging

__all__ = ['WorkerManager']

class WorkerManager(object):

    def __init__(self, conf):
        self.conf = conf
        self.idleWorkersQueue = Queue.Queue()
        self.workers = {} # 所有登记过的worker
        self.workersInQueue = {} # 记录所有在队列中的worker
        self.workerTask = {}
        self.lock = threading.Lock()

    def next_worker(self):
        '''返回一个空闲的worker，如果没有会阻塞程序执行'''
        while True:
            worker = self.idleWorkersQueue.get()
            if worker.get_uuid() in self.workers:
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
        id = worker.get_uuid()
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

    def assign_task(self, worker, task):
        """task分配给worker"""
        logging.info("%s -> %s",task.get_uuid(), str(worker))
        ok = False
        self.lock.acquire()
        if worker.get_uuid() not in self.workerTask:
            self.workerTask[worker.get_uuid()] = task
            ok = True
        self.lock.release()
        return ok

    def finish_task(self, worker, task):
        self.lock.acquire()
        self.workerTask.pop(worker.get_uuid(), None)
        self.lock.release()

    def get_workers(self):
        return self.workers.copy()

    def get_idle_workers(self):
        return self.workersInQueue.copy()

    def _is_die(self, worker):
        timestamp = common.get_timestamp()
        return timestamp - worker.get_heartbeat() > self.conf.DIE_THRESHOLD

    def clean_death_workers(self):
        '''定期检查worker的心跳信息，及时清除死亡worker，并返回死亡worker未完成作业'''
        self.lock.acquire()

        died_workers = set()
        unfinishedTask = set()
        for worker_id,worker in self.workers.items():
            if self._is_die(worker):
                died_workers.add(worker_id)
        for worker_id in died_workers:
            self.workers.pop(worker_id, None)
            self.workersInQueue.pop(worker_id, None)
            if worker_id in self.workerTask:
                unfinishedTask.add(self.workerTask.pop(worker_id))
        self.lock.release()

        return died_workers, unfinishedTask

