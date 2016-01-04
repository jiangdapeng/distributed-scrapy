#coding=utf8

MASTER_IP = 'localhost'
MASTER_PORT = 8000

# 心跳间隔(单位：秒)
HEARTBEAT_DURATION = 10

# 判断worker为死亡的阈值（单位：秒），建议设置为比心跳间隔大一点的值
DIE_THRESHOLD = 15