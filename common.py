#coding=utf8
import xmlrpclib, httplib

from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler

import time

def get_timestamp():
    return int(time.time())

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class NodeInfo(object):

    def __init__(self, name, ip, port, status, **kw):
        self.name = name
        self.ip = ip
        self.port = port
        self.status = status
        if 'heartbeat' in kw:
            self.heartbeat = kw.get('heartbeat')
        else:
            self.heartbeat = get_timestamp()

    def get_identifier(self):
        '''返回能唯一确定该节点的标识'''
        return '%s_%s_%s' % (self.name, self.ip, self.port)

    def get_heartbeat(self):
        '''返回最近一次心跳的时间'''
        return self.heartbeat

    def __str__(self):
        return self.get_identifier()

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

import socket

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
