# -*- coding: utf-8 -*-
''' 
@Time    : 2018/7/1 14:05
@Author  : xiao cai niao
@File    : zkhandle.py
@Software: PyCharm
'''

from kazoo.client import KazooClient
import sys
from .Loging import Logging

class zkHander(object):
    '''zookeeper系列操作'''
    def __init__(self,zk_hosts):
        self.zk = KazooClient(hosts=zk_hosts)
        self.path = '/mysql2iso'
        self.zk.start()
    def Exists(self,name):
        return self.zk.exists(path='{}/{}'.format(self.path,name))
    def Create(self,name):
        return self.zk.create(path='{}/{}'.format(self.path,name),value=b'',sequence=True)
    def init_node(self):
        state = self.zk.exists(path=self.path)
        if state is None:
            self.zk.create(path=self.path,value=b'',sequence=False)

    def CreateWatch(self,name,pt=None):
        '''创建watch'''
        online_state = self.zk.exists('{}/{}'.format(self.path , name))
        if online_state is not None:
            Logging(msg='repliction task watch for :{}'.format(name), level='info')
            @self.zk.DataWatch('{}/{}'.format(self.path , name))
            def my_func(data, stat,event):
                if data is None:
                    Logging(msg='replication task ({}) has been down!'.format(name),level='error')
                    if pt:
                        Logging(msg='closing thread {}'.format(pt), level='info')
                        pt.terminate()
                    self.zk.stop()
                    sys.exit()
        else:

            Logging(msg="this replication task {} node not exists".format(name),level='error')
            self.zk.stop()

    def close(self):
        self.zk.stop()
