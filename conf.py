# -*-coding=utf-8-*-
__author__ = 'jiangdapng(asuwill.jdp@gmail.com)'

MASTER_IP = 'localhost'
MASTER_PORT = 8000

# 心跳间隔(单位：秒)
HEARTBEAT_DURATION = 15

# 判断worker为死亡的阈值（单位：秒），建议设置为比心跳间隔大一点的值
DIE_THRESHOLD = 30


# worker default port
WORKER_PORT = 8001

PROJECTS_ROOT_PATH = 'E:/code/spiders'

# 同一个task中多个url分隔符
URLS_SEPERATOR = ';'
