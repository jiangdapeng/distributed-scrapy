# -*-coding=utf-8-*-
__author__ = 'jiangdapng(asuwill.jdp@gmail.com)'

import socket

from log import logging

class IPGetter(object):
    @classmethod
    def get_ip_address(cls):
        ip = socket.gethostbyname(socket.gethostname())
        return ip


if __name__ == '__main__':
    logging.info(IPGetter.get_ip_address())
