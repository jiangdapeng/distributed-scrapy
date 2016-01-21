#coding=utf8
import socket
import random
import xmlrpclib, httplib
from os import path

from conf import PROJECTS_ROOT_PATH

from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler

import time

__all__ = ['get_timestamp','RequestHandler','NodeStatus', 'NodeInfo', 'RPCServerProxy', 'doesServiceExist']

def get_timestamp():
    return int(time.time())

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class NodeStatus(object):
    idle = 1    # 空闲
    working = 2 # 工作中
    down = 4    # 无响应

class ClassFromDict(object):

    required_fields = []

    def __init__(self, **kwargs):
        pass

    @classmethod
    def from_dict(cls, attrs):
        ok = True
        for field in cls.required_fields:
            if field not in attrs:
                ok = False
                break
        if not ok:
            return None
        return cls(**attrs)


class NodeInfo(ClassFromDict):

    required_fields = ['name', 'ip', 'port', 'status']

    def __init__(self, name, ip, port, status, **kw):
        super(NodeInfo, self).__init__()
        self.name = name
        self.ip = ip
        self.port = port
        self.status = status
        if 'heartbeat' in kw:
            self.heartbeat = kw.get('heartbeat')
        else:
            self.heartbeat = get_timestamp()

    def get_uuid(self):
        '''返回能唯一确定该节点的标识'''
        return '%s_%s_%s' % (self.name, self.ip, self.port)

    def update_heartbeat(self):
        self.heartbeat = get_timestamp()

    def get_heartbeat(self):
        '''返回最近一次心跳的时间'''
        return self.heartbeat


    def __str__(self):
        return self.get_uuid()


class TimeoutTransport(xmlrpclib.Transport):
    timeout = 10.0
    def set_timeout(self, timeout):
        self.timeout = timeout
    def make_connection(self, host):
        h = httplib.HTTPConnection(host, timeout=self.timeout)
        return h

class RPCServerProxy(object):

    @classmethod
    def get_proxy(cls, node_info):
        return xmlrpclib.ServerProxy(cls.build_uri(node_info))

    @classmethod
    def build_uri(self, node_info):
        return 'http://%s:%s' % (node_info.ip, node_info.port)


def doesServiceExist(host, port):
    '''
    检测端口是否已经被使用
    :param host:
    :param port:
    :return:
    '''
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect((host, port))
        s.close()
        return True
    except:
        return False


class Task(ClassFromDict):
    """定义一个作业"""
    required_fields = ['uuid', 'project', 'spiderName', 'urls', 'params']


    def __init__(self, uuid, project, spiderName, urls, period= 24 * 60, params={}, priority=1, retry=0):
        """
        :param uuid: Task 唯一标识符
        :param project:
        :param spiderName:
        :param urls: 爬虫作业起始URL
        :param period: 执行周期，单位（分钟）
        :param params: 爬虫作业其他参数
        :param priority: 优先级
        :param retry: 重试次数
        :return:
        """
        super(Task, self).__init__()
        self.uuid = uuid
        self.project = project
        self.spiderName = spiderName
        self.urls = urls
        self.params = params
        self.period = period
        self.priority = priority
        self.retry = retry

    def get_uuid(self):
        return self.uuid

    def try_again(self):
        """是否重试"""
        if self.retry > 0:
            self.retry -= 1
            return True
        return False

    def get_cmd(self):
        urls = '-a urls="%s"' % ';'.join(self.urls)
        params = urls + ' ' +' '.join(['-a %s="%s"' % (k,v) for (k,v) in self.params.items()])
        crawlCmd = 'scrapy crawl {spider_name} {params}'.format(spider_name=self.spiderName, params=params)
        totalCmd = 'cd {path} && {crawlCmd}'.format(path=path.join(PROJECTS_ROOT_PATH,self.project), crawlCmd=crawlCmd)
        return totalCmd

    def __str__(self):
        return "Task{id=%s,project=%s}" % (self.uuid, self.project)


class TaskLoader(object):

    def __init__(self):
        pass

    def get_tasks(self):
        tasks = [Task(i, 'project1','spider1',urls=["http://www.baidu.com/s?wd=%s" % i,], params={}, priority=random.randrange(1,5)) for i in range(1000)]
        return tasks

class TaskStatus(object):
    """作业状态"""

    sucessed = 0        # 成功执行
    ready = 1           # 可以给worker执行了
    assigned = 2        # 已经成功给worker了
    failedToAssign = 4  # 提交给worker失败
    failedToExecute = 8 # worker执行失败
    notReturned = 16    # worker未反馈结果

class TaskResult(ClassFromDict):

    required_fields = ['taskUuid', 'returnCode']

    def __init__(self, taskUuid, returnCode, extra=''):
        super(TaskResult, self).__init__()

        self.taskUuid = taskUuid
        self.returnCode = returnCode
        self.extra = extra

    def is_success(self):
        return self.returnCode == 0

    def get_task_uuid(self):
        return self.taskUuid
