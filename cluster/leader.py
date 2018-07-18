# -*- coding: utf-8 -*-
''' 
@Time    : 2018/6/26 21:00
@Author  : Great God
@File    : leader.py
@Software: PyCharm
'''

import time,os,sys
from multiprocessing import Process

class Myprocess(Process):
    def __init__(self,name):
        super(Myprocess,self).__init__()
        self.name = name

    def run(self):
        while True:
            print('子进程:\033[1;32;40m%s\033[0m 父进程:\033[1;31;40m%s\033[0m' %(os.getpid(),os.getppid()))
            print('系统调用执行子进程，等待3秒')
            time.sleep(3)
            sys.exit()

if __name__ == '__main__':
    p1 = Myprocess('test')
    p1.start()
    a = 0
    while True:
        if a >= 10:
            print('测试关闭进程:{} 的pid:{}'.format(p1.name,p1.pid))
            p1.terminate()
            print('关闭成功  等待............')
            print(p1.is_alive())
            time.sleep(10)
            break
        #print('%s，的pid:\033[1;32;40m%s\033[0m' % (p1.name,p1.pid))
        time.sleep(1)
        a += 1