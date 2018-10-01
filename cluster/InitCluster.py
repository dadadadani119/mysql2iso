# -*- coding: utf-8 -*-
''' 
@Time    : 2018/6/27 20:21
@Author  : Great God
@File    : InitCluster.py
@Software: PyCharm
'''
import queue
import traceback,sys
from .ClusterZk import ClusterOp
from .ThreadDump import ThreadDump
from .ClusterHeart import HeartProtocol
sys.path.append("..")
from lib.Loging import Logging

global_queue = queue.Queue()

class ClusterEnt(HeartProtocol):
    def __init__(self,**kwargs):
        '''
        集群模式入口，所有心跳、leader选举、任务分配的入口
        :param kwargs:
        '''
        self.cluster_type = kwargs['cluster_type']
        del kwargs['cluster_type']
        if self.cluster_type == 'leader_mode':
            '''leader选举模式'''
            super(ClusterEnt,self).__init__()
            self.node_list = (kwargs['cluster_nodes']).split(',')
            self.host = kwargs['self_host']
            del kwargs['cluster_nodes']
            del kwargs['self_host']
            self.tasks = kwargs
        elif self.cluster_type == 'zk_mode':
            '''基于zookeeper的模式'''
            self.zk_hosts = kwargs['zk_hosts']
            del kwargs['zk_hosts']
        self.tasks = kwargs

    def __enter__(self):
        '''
        首先进行集群初始化，判断是否初次启动或宕机启动，如果宕机启动将获取cluster_status文件内容，判断是否有自己为leader的任务，
        有leader的任务将进行广播让其他节点接受，如果没有就直接广播信息获取最新数据
        :return:
        '''
        if self.cluster_type == 'leader_mode':
            s = self.protocoludp()
            p_ser = ThreadDump(socke=s,global_queue=global_queue)
            p_ser.start()
            if p_ser.is_alive():
                self.checkstatus()
                print(self.global_status)
                if self.global_status:
                    for _host in self.global_status:
                        if self.global_status[_host]['isself']:
                            for name in self.global_status[_host]['task_list']:
                                if self.global_status[_host]['task_list'][name] == 'LEADING':
                                    self.restartleader(name)
                        else:
                            self.restartleader(nodes=_host)
                else:
                    for task in self.tasks:
                        vote_result = self.vote(nodes=self.node_list,task_name=task)

            else:
                Logging(msg='starting udp server failed',level='error')
                sys.exit()
        elif self.cluster_type == 'zk_mode':
            with ClusterOp(task_list=self.tasks,zk_hosts=self.zk_hosts) as cz:
                pass


    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


