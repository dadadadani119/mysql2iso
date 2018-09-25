# -*- coding: utf-8 -*-
''' 
@Time    : 2018/6/26 21:52
@Author  : Great God
@File    : t2.py
@Software: PyCharm
'''

import socket

s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)

s.bind(('127.0.0.1',9999))
print("Bind UDP on prot:9999")

while True:
    data,addr=s.recvfrom(1024)
    print(eval(data))
    #print("Receive from %s:%s"% data)
    #s.sendto("Hello ".encode()+data,addr)