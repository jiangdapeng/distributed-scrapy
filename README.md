# distributed-scrapy

### 使用方式

#### 配置
- conf_master: IP\PORT，master使用的主机、端口 

- conf_client: IP\PORT，worker默认使用端口，如果在一台机器上启动多个client，则需要在命令行参数上分别指定端口

#### master机器

`python server.py`

#### worker机器
`python client.py`

或者

`pythoon client.py -p xxx`

master 和 worker可以在同一个主机上


### 采用Master-slave模式的简易分布式系统

开发目的：作为分布式爬虫系统的一个原型

#### Master

- 负责加载作业
- 接受作业结点的注册
- 给作业结点分配作业
- 接收作业结点返回的结果
- 定期根据心跳信息清理“死亡”作业结点

#### Woker

- 向Master报告自身存在（主动注册）
- 接受master分配的作业
- 完成作业
- 提交作业结果（状态）
- 定期向master提交心跳信息

### 相关技术

#### RPC

主从结点之间通信采用RPC。优点是：只需要定义好顶层接口就可以直接调用，无需关注网络层数据的传输。缺点是：Python的SimpleXMLRPCServer效率一般，
默认是单线程模式，虽然经过简单的改造就可以开启多线程模式，但是每秒能处理的连接数还是有限。

### 多线程

master：
- 提供RPC服务的线程
- 调度作业的线程
- 定期清理死亡结点的线程

worker：
- 提供RPC服务的线程（允许master向worker分配任务）
- 执行作业的线程（以及进程）
- 心跳线程
