# -*- coding: utf-8 -*-
''' 
@Time    : 2018/6/28 10:47
@Author  : Great God
@File    : ClusterHeart.py
@Software: PyCharm
'''

'''
该文件定义集群模式节点之间通讯协议以及心跳检查方法
'''
from kazoo.client import KazooClient
from kazoo.client import KazooState
import threading,traceback,time
import socket,sys
sys.path.append("..")
from lib.Loging import Logging
from .ClusterStatus import ClusterStatus

class HeartProtocol(ClusterStatus):
    def __init__(self):
        super(HeartProtocol,self).__init__()

        self.protocol = {
            'prototype' : None,                 #包类型【heart、leader、check,restart】,heart为节点之间的心跳检查包，无任何信息
                                                #leader类型代表进行leader选举包，包含replname以及nodes信息
                                                #check类型是对某个node宕机进行检查的包，针对某个node收到超过一半集群节点
                                                #的宕机检查包时将把该节点设置为宕机状态，且重新对该节点所属任务进行分配
                                                #restart是宕机重启检查到本地状态文件有自身为leader的任务，进行广播要求恢复leader
                                                #身份，可包含replname、nodes，如不包含replname代表重启获取最新节点信息
            'replname' : None,                  #同步任务名称
            'nodes' : [],                       #node列表,如果时leader选举列表首位代表选举的leader节点

            'acktype' : None                    #收到请求包应答的类型，和收到的prototype对应
        }


        self.host = None
        self.node_list = None

    def sendprotocol(self):
        '''
        集群中广播数据包
        :param node_list:
        :return:
        '''
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for host in self.node_list:
            s.sendto(str(self.protocol).encode(), (host, 9999))

    def restartleader(self,replname=None,nodes=None):
        '''
        宕机重启广播数据包,用于重新定义各个节点的状态
        :param replname:
        :return:
        '''
        self.protocol['prototype'] = 'restart'
        self.protocol['replname'] = replname
        self.protocol['nodes'] = nodes if nodes else [self.host]
        self.sendprotocol()

    def heart(self):
        '''
        广播心跳包
        :return:
        '''
        self.protocol['prototype'] = 'heart'
        self.sendprotocol()

    def nodecheck(self,node):
        '''
        广播某个节点宕机信息
        :return:
        '''
        self.protocol['prototype'] = 'check'
        self.protocol['nodes'] = [node]
        self.sendprotocol()

    def leadervote(self,replname,nodes=None):
        '''
        广播leader选举结果
        :return:
        '''
        self.protocol['prototype'] = 'leader'
        self.protocol['replname'] = replname
        self.protocol['nodes'] = nodes if nodes else [self.host]
        self.sendprotocol()

    def protocoludp(self):
        '''
        采用UDP协议，client端无需等待确认是否收到
        如果采用tcp需要确定是否链接建立以及是否收到，这样直接影响效率以及不必要的报错
        使用UDP可以不管对方是否有监听或者是否收到广播信息，节点存活检查都使用接收到的
        数据包进行判断
        :return:
        '''
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.bind(('0.0.0.0', 9999))
            Logging(msg='Bind UDP on prot:9999, listening 0.0.0.0',level='error')
            return s
        except:
            Logging(msg=traceback.format_exc(),level='error')

    def preparepack(self,pack):
        '''
        对收到的数据进行protocol解析
        :param pack:
        :return:
        '''
        _pack = eval(pack)
        for key in _pack:
            if key in self.protocol:
                self.protocol['key'] = _pack[key]



class zkheartbeat:
    def __init__(self,zk_hosts,task_name):
        self.task_name = task_name
        self.zk = KazooClient(hosts=zk_hosts)
        self.zk.start()
        self.retry_state = ''

    def retry_create(self):
        '''创建临时node'''
        node_stat = self.zk.exists(path='/mysql2iso/' + self.task_name)
        if node_stat is None:
            self.zk.create(path="/mysql2iso/" + self.task_name, value="", ephemeral=True)
        else:
            self.zk.delete(path="/mysql2iso/" + self.task_name)
            self.zk.create(path="/mysql2iso/" + self.task_name, value="", ephemeral=True)
        self.retry_state = ''

    def run(self):
        self.add_linsten()
        try:
            self.zk.create(path="/mysql2iso/" + self.task_name, value=b"", ephemeral=True)
            while True:
                if self.retry_state == 'Connected':
                    self.retry_create()
                time.sleep(1)
        except:
            Logging(msg=traceback.format_exc(),level='warning')
            sys.exit()

    def add_linsten(self):
        @self.zk.add_listener
        def my_listener(state):
            if state == KazooState.LOST:
                Logging(msg='Zookeeper session lost, state: CLOSED',level='info')
            elif state == KazooState.SUSPENDED:
                Logging(msg='Zookeeper session lost, state: SUSPENDED',level='info')
            else:
                Logging(msg='Zookeeper session lost, state: Connected',level='info')
                self.retry_state = "Connected"

