# -*- coding: utf-8 -*-
''' 
@Time    : 2018/7/1 16:57
@Author  : Great God
@File    : ClusterZk.py
@Software: PyCharm
'''
import threading
from multiprocessing import Process
import sys,time,random
from contextlib import closing
from .ClusterHeart import zkheartbeat
sys.path.append("..")
from lib.zkhandle import zkHander
from lib.Loging import Logging
from lib.ThreadManager import ThreadDump
from multiprocessing import Queue
#import queue

my_queue = Queue(2048)  # 同步线程与目标库执行线程之间的通信队列，队列中存储以GTID为单位的sql数据内容，最大存储2048个GTID事务

class ClusterOp:
    def __init__(self,task_list,zk_hosts):
        self.task_list = task_list
        self.zk_hosts = zk_hosts
        self.task_thread_list = {}
        self.watch_thread_list = {}
        self.heart_thread_list = {}


    def __enter__(self):
        '''
        启动任务或监听任务
        :return:
        '''
        with closing(zkHander(self.zk_hosts)) as zkhandle:
            zkhandle.init_node()
        for task_name in self.task_list:
            self.__create_repl(task_name=task_name)
        self.__check_state()

    def __check_state(self):
        while True:

            if self.task_thread_list:
                #不管源库读取任务、目标库操作任务其中一个宕机将直接退出任务
                for task_name in self.task_thread_list:
                    for thread in self.task_thread_list[task_name]:
                        if thread.is_alive():
                            pass
                        else:
                            Logging(msg='replication thread {} is down '.format(self.task_thread_list[task_name]),level='error')
                            for t in self.task_thread_list[task_name]:
                                try:
                                    t.terminate()
                                except:
                                    pass
                            del self.task_thread_list[task_name]
                            Logging(msg='stop heart thread {} is down '.format(self.heart_thread_list[task_name]),level='error')
                            self.heart_thread_list[task_name].terminate()
                            del self.heart_thread_list[task_name]
            if self.watch_thread_list:
                for task_name in self.watch_thread_list:
                    if self.watch_thread_list[task_name].is_alive():
                        pass
                    else:
                        del self.watch_thread_list[task_name]
                        self.__create_repl(task_name=task_name)

            time.sleep(1)

    def __create_repl(self,task_name):
        '''
        创建心跳及同步任务
        同步任务在完成之后会sleep一段时间，防止多个任务被同一节点启动
        :param task_name:
        :return:
        '''
        with closing(zkHander(self.zk_hosts)) as zkhandle:
            task_state = zkhandle.Exists(task_name)
        if task_state:
            watch_p = ThreadDump(type='watch')
            watch_p.start()
            zkhandle = zkHander(zk_hosts=self.zk_hosts)
            zkhandle.CreateWatch(name=task_name, pt=watch_p)
            self.watch_thread_list[task_name] = watch_p
        else:
            #启动心跳线程,随机sleep一段时间，防止多个同时去创建
            time.sleep(random.uniform(1, 3))
            heart_p = ThreadDump(task_name=task_name, zk_hosts=self.zk_hosts, type='heart')
            heart_p.start()
            self.heart_thread_list[task_name] = heart_p
            # 启动任务
            if heart_p.is_alive():
                #源库同步线程
                task_p = ThreadDump(task_name=task_name, type='repl',_argv=dict(self.task_list[task_name],**{'queue':my_queue}))
                task_p.start()
                #目标库写入线程
                task_d = ThreadDump(task_name=task_name, type='repl_des',_argv=dict(self.task_list[task_name],**{'queue':my_queue}))
                task_d.start()

                self.task_thread_list[task_name] = [task_p,task_d]
            time.sleep(60)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass



# # class ThreadDump(Process):
# class ThreadDump(threading.Thread):
#     def __init__(self,type=None,task_name=None,zk_hosts=None,_argv=None):
#         #super(ThreadDump, self).__init__()
#         threading.Thread.__init__(self)
#         self.type = type            #类型，watch、repl、heart
#         self.task_name = task_name
#         self.zk_hosts = zk_hosts
#         self._argv = _argv
#     def run(self):
#         '''
#         启动监听或者服务线程
#         :return:
#         '''
#         if self.type == 'watch':
#             i = 0
#             while i < 1000000:
#                 time.sleep(3600)
#                 i += 1
#         elif self.type == 'repl':
#             from lib.entrance import Entrance
#             with Entrance(self._argv) as en:
#                 pass
#         elif self.type == 'repl_des':
#             from lib.destination import destination
#             with destination(**self._argv) as des:
#                 pass
#         elif self.type == 'heart':
#             zkheartbeat(zk_hosts=self.zk_hosts,task_name=self.task_name).run()
