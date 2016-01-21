# -*-coding=utf-8-*-
__author__ = 'jiangdapng(asuwill.jdp@gmail.com)'

import Queue
import threading
from log import logging

class TaskManager(object):
    """集中管理作业，包括作业加载，分配，回收等"""

    def __init__(self, taskLoader):
        self.taskLoader = taskLoader
        self.lock = threading.Lock()
        self.taskQueue = Queue.PriorityQueue()
        self.runningTasks = {}

    def load_tasks(self):
        if self.taskLoader is not None:
            tasks = self.taskLoader.get_tasks()
            for task in tasks:
                self.taskQueue.put((task.priority,task))

    def next_task(self):
        """返回下一个要执行的作业"""
        task = self.taskQueue.get()
        return task[1]

    def finish_task(self, task):
        """完成一个作业"""
        logging.info("task finished: %s", str(task))

    def fail_task(self, task):
        """告知该作业执行失败"""
        self.taskQueue.put((task.priority,task))

    def get_task_count(self):
        return self.taskQueue.qsize()
