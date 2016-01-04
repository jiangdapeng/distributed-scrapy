#coding=utf8
import Queue
import xmlrpclib
import threading
import traceback
import time

import common
import conf_master

class Task(object):

	def __init__(self, identifier, project, spider_name, urls):
		self.identifier = identifier
		self.project = project
		self.spider_name = spider_name
		self.urls = urls

	def get_identifier(self):
		return self.identifier

class TaskLoader(object):

	def __init__(self):
		pass

	def get_tasks(self):
		tasks = [Task(i, 'test','test_spider',['http://'+str(i)]) for i in range(1000)]
		return tasks


class CheckWorkersThread(threading.Thread):
	'''用于定期检查作业节点状态，并清除死亡节点的线程'''
	def __init__(self, master):
		threading.Thread.__init__(self)

		self.master = master

	def run(self):
		while True:
			try:
				time.sleep(conf_master.DIE_THRESHOLD)
				self.master.clean_die_worker()
			except Exception,e:
				traceback.print_exc()


class Master(object):

	def __init__(self, task_loader, conf):
		self.lock = threading.Lock()
		self.idle_workers = Queue.Queue()
		self.working_workers = {}
		self.workers = {}
		self.tasks = Queue.Queue()
		self.running_tasks = {}
		self.task_loader = task_loader
		self.conf = conf
		self.load_tasks()

	def get_status(self):
		return {
			'total_workers': self.workers,
			'tasks': self.tasks.qsize(),
			'idle_workers': self.idle_workers.qsize()
		}

	def is_die(self, worker):
		timestamp = common.get_timestamp()
		return timestamp - worker.get_heartbeat() > self.conf.DIE_THRESHOLD

	def clean_die_worker(self):
		'''定期检查worker的心跳信息，及时清除死亡worker'''

		self.lock.acquire()

		died_workers = set()
		for worker_id,worker in self.workers.items():
			if self.is_die(worker):
				died_workers.add(worker_id)
		for worker_id in died_workers:
			self.workers.pop(worker_id, None)

		self.lock.release()


	def register_worker(self, worker):
		identifier = worker.get_identifier()
		self.lock.acquire()
		self.workers[identifier] = worker
		self.idle_workers.put(worker);
		self.lock.release()
		status = "OK"
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

	def task_complete(self, worker, task, stats):
		task_id = task.get_identifier()
		worker_id = worker.get_identifier()

		self.lock.acquire()
		if task_id in self.running_tasks:
			self.running_tasks.pop(task_id)
		if worker_id in self.working_workers:
			self.working_workers.pop(worker_id)
		self.workers[worker_id] = worker
		self.idle_workers.put(worker)
		self.lock.release()

		return True

	def heartbeat(self, worker_info):
		'''收到心跳信息，更新该工作节点的信息'''
		self.lock.acquire()

		self.workers[worker_info.get_identifier()] = worker_info

		self.lock.release()
		return True

	def lookup_spider(self, spider):
		pass

	def load_tasks(self):
		if self.task_loader is not None:
			tasks = self.task_loader.get_tasks()
			for task in tasks:
				self.tasks.put(task)

	def schedule_next(self):
		print('qsize:',self.tasks.qsize())
		task = self.tasks.get()
		worker = self.idle_workers.get()
		worker_id = worker.get_identifier()
		try:
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
		except Exception,e:
			traceback.print_exc()
			self.tasks.put(task)
			self.workers.pop(worker_id, None)

	def serve_forever(self):
		check_thread = CheckWorkersThread(self)
		check_thread.start()

		while True:
			try:
				self.schedule_next()
			except Exception,e:
				traceback.print_exc()

	