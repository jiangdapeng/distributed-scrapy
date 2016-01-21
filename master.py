#coding=utf8
import Queue
import threading
import traceback
import time

import common
from common import TaskStatus
import conf
from worker_manager import WorkerManager
from task_manager import TaskManager
from log import logging



class CheckWorkersThread(threading.Thread):
    '''用于定期检查作业节点状态，并清除死亡节点的线程'''
    def __init__(self, master):
        threading.Thread.__init__(self)

        self.master = master

    def run(self):
        while True:
            try:
                time.sleep(conf.DIE_THRESHOLD)
                self.master.clean_death_workers()
            except Exception,e:
                traceback.print_exc()


class Master(object):

    def __init__(self, task_loader, conf):
        self.lock = threading.Lock()
        self.workerManager = WorkerManager(conf)
        self.taskManager = TaskManager(task_loader)
        self.running_tasks = {}
        self.conf = conf
        self.load_tasks()

    def get_status(self):
        return {
            'total_workers': self.workerManager.get_workers(),
            'tasks': self.taskManager.get_tasks_stats(),
            'idle_workers': self.workerManager.get_idle_workers()
        }


    def clean_death_workers(self):
        '''定期检查worker的心跳信息，及时清除死亡worker'''
        workers,tasks = self.workerManager.clean_death_workers()
        logging.info("death workers:%s; relatedTasks:%s", workers, tasks)
        for task in tasks:
            self.taskManager.fail_task(task.uuid, TaskStatus.notReturned)
        return workers

    def register_worker(self, worker):
        '''注册作业节点'''
        logging.info("%s come in", worker)
        status = "OK"
        if worker is not None:
            self.workerManager.add_worker(worker)
        else:
            status = "Invalid"
        # logging.info(self.workerManager.get_workers())
        return status
        
    def remove_worker(self, worker):
        status = "OK"
        if worker is None:
            status = "Invalid"
            return status
        identifier = worker.get_uuid()
        w, tasks = self.workerManager.remove_worker(identifier)
        for task in tasks:
            self.taskManager.fail_task(task.get_uuid(), TaskStatus.notReturned)
        if w is None:
            status = "NOT EXISTS"
        return status

    def task_complete(self, worker, taskResult):
        '''worker完成一个作业，返回作业统计信息，worker重新归于队列'''
        self.workerManager.finish_task(worker, taskResult)
        self.workerManager.update_worker(worker)
        if taskResult.is_success():
            self.taskManager.finish_task(taskResult.get_task_uuid())
        else:
            self.taskManager.fail_task(taskResult.get_task_uuid(), TaskStatus.failedToExecute)
        return True

    def heartbeat(self, worker):
        '''收到心跳信息，更新该工作节点的信息'''
        self.workerManager.update_worker(worker)
        return True

    def lookup_spider(self, spider):
        pass

    def load_tasks(self):
        self.taskManager.load_tasks()

    def schedule_next(self):
        logging.info('tasks: %s',self.taskManager.get_tasks_stats())
        task = self.taskManager.next_task()
        worker = self.workerManager.next_worker()
        self.workerManager.assign_task(worker, task)
        try:
            proxy = common.RPCServerProxy.get_proxy(worker)
            proxy.assign_task(task)
        except Exception,e:
            traceback.print_exc()
            self.remove_worker(worker)

    def serve_forever(self):
        check_thread = CheckWorkersThread(self)
        check_thread.start()

        while True:
            try:
                self.schedule_next()
            except Exception,e:
                traceback.print_exc()

    