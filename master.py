#coding=utf8
import Queue
import threading
import traceback
import time

import common
import conf_master
from worker_manager import WorkerManager


class CheckWorkersThread(threading.Thread):
    '''用于定期检查作业节点状态，并清除死亡节点的线程'''
    def __init__(self, master):
        threading.Thread.__init__(self)

        self.master = master

    def run(self):
        while True:
            try:
                time.sleep(conf_master.DIE_THRESHOLD)
                self.master.clean_death_workers()
            except Exception,e:
                traceback.print_exc()


class Master(object):

    def __init__(self, task_loader, conf):
        self.lock = threading.Lock()
        self.workerManager = WorkerManager(conf)
        self.tasks_queue = Queue.Queue()
        self.running_tasks = {}
        self.task_loader = task_loader
        self.conf = conf
        self.load_tasks()

    def get_status(self):
        return {
            'total_workers': self.workerManager.get_workers(),
            'tasks': self.tasks_queue.qsize(),
            'idle_workers': self.workerManager.get_idle_workers()
        }


    def clean_death_workers(self):
        '''定期检查worker的心跳信息，及时清除死亡worker'''
        return self.workerManager.clean_death_workers()


    def register_worker(self, worker):
        '''注册作业节点'''
        print('in master:',worker)
        status = "OK"
        if worker is not None:
            self.workerManager.add_worker(worker)
        else:
            status = "Invalid"
        print(status)
        print(self.workerManager.get_workers())
        return status
        
    def remove_worker(self, worker):
        status = "OK"
        if worker is None:
            status = "Invalid"
            return status
        identifier = worker.get_identifier()
        w = self.workerManager.remove_worker(identifier)
        if w is None:
            status = "NOT EXISTS"
        return status

    def task_complete(self, worker, task, stats):
        '''worker完成一个作业，返回作业统计信息，worker重新归于队列'''
        task_id = task.get_identifier()

        worker.status = common.NodeStatus.idle
        self.workerManager.update_worker(worker)

        self.lock.acquire()
        if task_id in self.running_tasks:
            self.running_tasks.pop(task_id)
        self.lock.release()

        return True

    def heartbeat(self, worker):
        '''收到心跳信息，更新该工作节点的信息'''
        self.workerManager.update_worker(worker)
        return True

    def lookup_spider(self, spider):
        pass

    def load_tasks(self):
        if self.task_loader is not None:
            tasks = self.task_loader.get_tasks()
            for task in tasks:
                self.tasks_queue.put(task)

    def schedule_next(self):
        print('qsize:',self.tasks_queue.qsize())
        task = self.tasks_queue.get()
        worker = self.workerManager.next_worker()
        worker_id = worker.get_identifier()
        print(task.get_identifier(), str(worker))

        try:
            proxy = common.RPCServerProxy.get_proxy(worker)
            r = proxy.assign_task(task)
            if r == True:
                # 分发任务成功
                self.lock.acquire()
                self.running_tasks[task.identifier] = task
                self.lock.release()
            else:
                # 分发失败，重新放入作业队列
                self.tasks_queue.put(task)
        except Exception,e:
            traceback.print_exc()
            self.tasks_queue.put(task)
            self.workerManager.remove_worker(worker_id)

    def serve_forever(self):
        check_thread = CheckWorkersThread(self)
        check_thread.start()

        while True:
            try:
                self.schedule_next()
            except Exception,e:
                traceback.print_exc()

    