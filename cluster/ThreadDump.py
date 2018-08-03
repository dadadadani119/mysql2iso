# -*- coding: utf-8 -*-
''' 
@Time    : 2018/7/1 17:04
@Author  : Great God
@File    : ThreadDump.py
@Software: PyCharm
'''
import threading

class ThreadDump(threading.Thread):
    def __init__(self,socke=None,global_queue=None):
        threading.Thread.__init__(self)
        self.s = socke
        self.global_queue = global_queue

    def run(self):
        '''
        启动服务端一直监听各节点发送的消息并放入队列
        :return:
        '''
        while True:
            data, addr = self.s.recvfrom(1024)
            self.global_queue.put([addr,data])