# -*- coding: utf-8 -*-
''' 
@Time    : 2018/7/25 13:44
@Author  : Great God
@File    : SingeTask.py
@Software: PyCharm
'''

from .ThreadManager import ThreadDump
import sys,time
from multiprocessing import Queue
from lib.Loging import Logging


my_queue = Queue(2048)

class SingeTask:
    def __init__(self,**kwargs):
        self.kwargs = kwargs
        self.thread_list = []   #同步及追加线程

    def __enter__(self):
        # 源库同步线程
        task_p = ThreadDump(type='repl',_argv=dict(self.kwargs, **{'queue': my_queue}))
        task_p.start()
        # 目标库写入线程
        task_d = ThreadDump(type='repl_des',_argv=dict(self.kwargs, **{'queue': my_queue}))
        task_d.start()

        self.thread_list = [task_p, task_d]
        self.__check_thread_state()
    def __check_thread_state(self):
        while 1:
            for thread in self.thread_list:
                #print(thread,thread.is_alive())
                if not thread.is_alive():
                    Logging(msg='replication thread {} is down,stop all thread!!'.format(thread),level='error')
                    self.__stop_all_thread()
                    sys.exit()
            time.sleep(1)

    def __stop_all_thread(self):
        for thread in self.thread_list:
            if thread.is_alive():
                thread.terminate()


    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
