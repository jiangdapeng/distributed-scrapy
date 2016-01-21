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
    down = 3    # 无响应

class NodeInfo(object):

    required_fields = ['name', 'ip', 'port', 'status']

    def __init__(self, name, ip, port, status, **kw):
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

    @classmethod
    def from_dict(cls, infoDict):
        ok = True
        for f in cls.required_fields:
            if f not in infoDict:
                ok = False
                break
        if not ok:
            return None
        node = cls(**infoDict )
        return node

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


class Task(object):
    """定义一个作业"""
    required_fields = ['uuid', 'project', 'spider_name', 'urls', 'params', 'period', 'priority']

    def __init__(self, uuid, project, spider_name, urls, period= 24 * 60, params={}, priority=1):
        self.uuid = uuid
        self.project = project
        self.spider_name = spider_name
        self.urls = urls
        self.params = params
        self.period = period
        self.priority = priority

    def get_uuid(self):
        return self.uuid


    def get_cmd(self):
        urls = '-a urls="%s"' % ';'.join(self.urls)
        params = urls + ' ' +' '.join(['-a %s="%s"' % (k,v) for (k,v) in self.params.items()])
        crawlCmd = 'scrapy crawl {spider_name} {params}'.format(spider_name=self.spider_name, params=params)
        totalCmd = 'cd {path} && {crawlCmd}'.format(path=path.join(PROJECTS_ROOT_PATH,self.project), crawlCmd=crawlCmd)
        return totalCmd

    def __str__(self):
        return "Task{id=%s,project=%s}" % (self.uuid, self.project)

    @classmethod
    def from_dict(cls, taskInfo):
        valid = True
        for field in cls.required_fields:
            if field not in taskInfo:
                valid = False
                break
        if not valid:
            return None

        return cls(**taskInfo)

class TaskLoader(object):

    def __init__(self):
        pass

    def get_tasks(self):
        tasks = [Task(i, 'test','test_spider',urls=["http://tieba.baidu.com/%s" % i,], params={}, priority=random.randrange(1,5)) for i in range(1000)]
        return tasks
