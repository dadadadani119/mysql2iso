# -*- coding: utf-8 -*-
''' 
@Time    : 2018/7/25 13:33
@Author  : Great God
@File    : ThreadManager.py
@Software: PyCharm
'''
import time
#import threading
from multiprocessing import Process
from cluster.ClusterHeart import zkheartbeat


#class ThreadDump(threading.Thread):
class ThreadDump(Process):
    def __init__(self,type=None,task_name=None,zk_hosts=None,_argv=None):
        super(ThreadDump, self).__init__()
        #threading.Thread.__init__(self)
        self.type = type            #类型，watch、repl、heart
        self.task_name = task_name
        self.zk_hosts = zk_hosts
        self._argv = _argv
    def run(self):
        '''
        启动监听或者服务线程
        :return:
        '''
        if self.type == 'watch':
            i = 0
            while i < 1000000:
                time.sleep(3600)
                i += 1
        elif self.type == 'repl':
            from lib.entrance import Entrance
            with Entrance(self._argv) as en:
                pass
        elif self.type == 'repl_des':
            from lib.destination import destination
            with destination(**self._argv) as des:
                pass
        elif self.type == 'heart':
            zkheartbeat(zk_hosts=self.zk_hosts,task_name=self.task_name).run()
