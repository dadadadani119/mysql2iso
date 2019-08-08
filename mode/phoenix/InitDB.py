# -*- coding: utf-8 -*-
''' 
@Time    : 2018/8/15 11:17
@Author  : xiao cai niao
@File    : InitDB.py
@Software: PyCharm
'''
import sys,traceback
import phoenixdb
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
            database_url = 'http://{}:{}/'.format(self.host,self.port)
            conn = phoenixdb.connect(database_url, autocommit=True)
            return conn
        except:
            Logging(msg=traceback.format_exc(),level='error')
            return None