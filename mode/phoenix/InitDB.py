# -*- coding: utf-8 -*-
''' 
@Time    : 2018/8/15 11:17
@Author  : Great God
@File    : InitDB.py
@Software: PyCharm
'''
import sys,traceback
import jaydebeapi as jdbc
sys.path.append("..")
from lib.Loging import Logging

class InitDB:
    def __init__(self,jar,host,port,jar_conf={},user = None,passwd=None):
        self.jar = jar
        self.jar_conf = jar_conf
        self.host = host
        self.port = port
        self.user = '' if user in ('\'\'','""') else user
        self.passwd = '' if passwd in ('\'\'','""') else passwd

    def Init(self):
        try:
            conn = jdbc.connect('org.apache.phoenix.jdbc.PhoenixDriver',
                         'jdbc:phoenix:{}:{}'.format(self.host,self.port),
                         dict(self.jar_conf,**{'username':self.user,'password':self.passwd}),
                                ['{}'.format(self.jar)])
            return conn
        except:
            Logging(msg=traceback.format_exc(),level='error')
            return None