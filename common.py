import xmlrpclib, httplib

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class NodeInfo(object):

	def __init__(self, name, ip, port, status):
		self.name = name
		self.ip = ip
		self.port = port
		self.status = status

	def get_identifier(self):
		return '%s_%s_%s' % (self.name, self.ip, self.port)

	def __str__(self):
		return get_identifier()

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
