#coding=utf8
import Queue
import xmlrpclib
import threading

import common

class Task(object):

	def __init__(self, identifier, project, spider_name, urls):
		self.identifier = identifier
		self.project = project
		self.spider_name = spider_name
		self.urls = urls

class TaskLoader(object):

	def __init__(self):
		pass

	def get_tasks(self):
		tasks = [Task(i, 'test','test_spider',[]) for i in range(10)]
		return tasks


class Master(object):

	def __init__(self, task_loader):
		self.lock = threading.Lock()
		self.idle_workers = Queue.Queue()
		self.working_workers = {}
		self.workers = {}
		self.tasks = Queue.Queue()
		self.running_tasks = {}
		self.task_loader = task_loader
		self.load_tasks()

	def register_worker(self, worker):
		identifier = worker.get_identifier()
		self.lock.acquire()
		if identifier in self.workers:
			status =  "EXISTS"
		else:
			self.workers[identifier] = worker
			
			self.idle_workers.put(worker);
			status = "OK"
		self.lock.release()
		return status
		
	def remove_worker(self, worker):
		identifier = worker.get_identifier()
		status = "OK"
		self.lock.acquire()
		if identifier in self.workers:
			self.workers.pop(identifier)
		else:
			status = "NOT EXISTS"
		if identifier in self.working_workers:
			self.working_workers.pop(identifier)
		self.lock.release()
		return status

	def task_done(self, worker, task, stats):
		task_id = task.identifier
		worker_id = worker.get_identifier

		self.lock.acquire()
		if task_id in self.running_tasks:
			self.running_tasks.pop(task_id)
		if worker_id in self.working_workers:
			self.working_workers.pop(worker_id)
		self.idle_workers.put(worker)
		self.lock.release()

	def lookup_spider(self, spider):
		pass

	def load_tasks(self):
		if self.task_loader is not None:
			tasks = self.task_loader.get_tasks()
			for task in tasks:
				self.tasks.put(task)

	def schedule_next(self):
		task = self.tasks.get()
		worker = self.idle_workers.get()
		worker_id = worker.get_identifier()
		proxy = common.RPCServerProxy.get_proxy(worker)
		r = proxy.assign_task(task)
		if r == True:
			# 分发任务成功
			self.lock.acquire()

			self.running_tasks[task.identifier] = task
			self.working_workers[worker_id] = worker

			self.lock.release()
		else:
			# 分发失败，重新放入作业队列
			self.tasks.put(task)		

	def serve_forever(self):
		while True:
			self.schedule_next()

	