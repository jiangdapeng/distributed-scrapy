# -*-coding=utf-8-*-
__author__ = 'jiangdapng(asuwill.jdp@gmail.com)'

import Queue
import threading
from log import logging
from common import TaskStatus

class TaskManager(object):
    """集中管理作业，包括作业加载，分配，回收等"""

    def __init__(self, taskLoader):
        self.taskLoader = taskLoader
        self.lock = threading.Lock()
        self.taskQueue = Queue.PriorityQueue()
        self.runningTasks = {}
        self.failedTasks = set()


    def load_tasks(self):
        if self.taskLoader is not None:
            tasks = self.taskLoader.get_tasks()
            for task in tasks:
                self.taskQueue.put((task.priority,task))

    def next_task(self):
        """返回下一个要执行的作业"""
        pri,task = self.taskQueue.get()
        self.lock.acquire()
        self.runningTasks[task.get_uuid()] = task
        self.lock.release()
        return task

    def finish_task(self, taskUuid):
        """完成一个作业"""
        logging.info("task finished: %s", str(taskUuid))
        self.lock.acquire()
        self.runningTasks.pop(taskUuid, None)
        self.lock.release()

    def fail_task(self, taskUuid, status):
        """告知该作业执行失败"""
        self.lock.acquire()
        task = self.runningTasks.pop(taskUuid, None)
        self.lock.release()
        if task is not None:
            if status in (TaskStatus.failedToAssign, TaskStatus.notReturned):
                self.taskQueue.put((task.priority,task))
            elif status == TaskStatus.failedToExecute:
                if task.try_again():
                    self.taskQueue.put((task.prority,task))
                else:
                    self.failedTasks.add(task)


    def get_tasks_stats(self):
        """返回等待作业、正在运行作业以及失败作业的个数"""
        return {
            "waiting_tasks": self.taskQueue.qsize(),
            "running_tasks": len(self.runningTasks),
            "failed_task": len(self.failedTasks)
        }
