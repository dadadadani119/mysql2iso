# -*- coding: utf-8 -*-
''' 
@Time    : 2018/7/1 16:57
@Author  : xiao cai niao
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
from lib.TcpManager import manager
from multiprocessing import Queue
import socket

BUFSIZE=1024
encoding='utf8'
master_thread_list = []                 #主从操作主进程信息
put_task_list = []                      #记录客户端put上来的数据

my_queue = Queue(2048)  # 同步线程与目标库执行线程之间的通信队列，队列中存储以GTID为单位的sql数据内容，最大存储2048个GTID事务

class ClusterOp:
    def __init__(self,task_list,zk_hosts,port):
        self.task_list = task_list
        self.zk_hosts = zk_hosts
        self.port = port
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
            if self.__create_watch(task_name=task_name) is None:
                self.__create_task(task_name=task_name)
            #self.__create_repl(task_name=task_name)
        self.__check_state()

    def __check_state(self):

        '''启动服务监听'''
        lst = Listener(self.port)
        lst.start()
        ''''''

        while True:
            ''''''
            self.__check_new_task()
            self.__check_task_state()
            ''''''

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
                                    for idx,row in enumerate(master_thread_list):
                                        if row['task_name'] == task_name:
                                            row['state'] == 'crash'
                                            master_thread_list[idx] == row.copy()
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

    def __check_new_task(self):
        while 1:
            if put_task_list:
                master_thread_list.append(put_task_list[0])
                del put_task_list[0]
            else:
                break

    def __check_task_state(self):
        '''
        数据列表状态于线程列表对比
        :return:
        '''
        for idx,task in enumerate(master_thread_list):
            if task['state'] == 'started':
                if task['task_name'] not in self.task_thread_list:
                    Logging(msg='start task for {} now....'.format(task['task_name']),level='info')
                    self.__create_task(task_name=task['task_name'])
            elif task['state'] == 'watch':
                if task['task_name'] not in self.watch_thread_list:
                    Logging(msg='start watch task for {} now....'.format(task['task_name']),level='info')
                    if self.__create_watch(task_name=task['task_name']):
                        Logging(msg='watch success .......',level='info')
                        task['start_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        master_thread_list[idx] = task.copy()
                    else:
                        Logging(msg='watch is failed, because this task not exists for zookeepr,start task for now....',level='warning')

            elif task['state'] == 'stoped':
                if task['task_name'] in self.task_thread_list:
                    for t in self.task_thread_list[task['task_name']]:
                        t.terminate()
                    self.heart_thread_list['task_name'].terminate()
                elif task['task_name'] in self.watch_thread_list:
                    self.watch_thread_list[task['task_name']].terminate()

    def __create_watch(self,task_name):
        '''
        检查任务是否存在，并监听
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
            return True
        else:
            return None

    def __create_task(self,task_name):
        '''
        启动任务所需的同步、写入主进程
        :param task_name:
        :return:
        '''
        heart_p = ThreadDump(task_name=task_name, zk_hosts=self.zk_hosts, type='heart')
        heart_p.start()
        self.heart_thread_list[task_name] = heart_p
        # 启动任务
        if heart_p.is_alive():
            # 源库同步线程
            task_p = ThreadDump(task_name=task_name, type='repl',
                                _argv=dict(self.task_list[task_name], **{'queue': my_queue}))
            task_p.start()
            # 目标库写入线程
            task_d = ThreadDump(task_name=task_name, type='repl_des',
                                _argv=dict(self.task_list[task_name], **{'queue': my_queue}))
            task_d.start()

            self.task_thread_list[task_name] = [task_p, task_d]

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class Reader(threading.Thread):
    def __init__(self, client,client_host):
        threading.Thread.__init__(self)
        self.client = client
        self.client_host = client_host

    def run(self):
        global master_thread_list,put_task_list
        while True:
            data = self.client.recv(BUFSIZE)
            string = bytes.decode(data, encoding)
            if string:
                cmd = string.split()
                m = manager(put_task_list,master_thread_list,self.client,cmd)
                m.case_list.get(cmd[0])()
            else:
                Logging(msg='client:{} is closing ...'.format(self.client_host), level='info')
                break


class Listener(threading.Thread):
    def __init__(self, port):
        threading.Thread.__init__(self)
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", port))
        self.sock.listen(0)

    def run(self):
        Logging(msg='server listener started for port: {}'.format(self.port), level='info')
        while True:
            client, cltadd = self.sock.accept()
            Logging(msg='accept a connect from {}:{}'.format(cltadd[0],cltadd[1]), level='info')
            Reader(client,cltadd[0]).start()

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
