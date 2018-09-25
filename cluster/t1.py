# -*- coding: utf-8 -*-
''' 
@Time    : 2018/6/26 21:41
@Author  : Great God
@File    : t1.py
@Software: PyCharm
'''

import socket
import time
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
a = {'host':'10.6.10.11:9011','task_list':{'name1':'FOLLOWING','name2':'LEADING'},'status':'online','isself':1}
for data in ["chenshan", "yuanhui", "chendianqiang"]:
    s.sendto(str(a).encode(), ('127.0.0.1', 9999))
    print(data)
    #time.sleep(1)

s.close()