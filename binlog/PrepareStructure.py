# -*- encoding: utf-8 -*-
'''
@author: Great God
'''

import sys
import pymysql
import time,traceback
sys.path.append("..")
from lib.InitDB import InitMyDB
from lib.Loging import Logging

class GetStruct:
    def __init__(self,host=None,port=None,user=None,passwd=None,socket=None,binlog=None):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.socket = socket
        self.binlog = binlog

        self.connection = InitMyDB(mysql_host=self.host,mysql_port=self.port,mysql_user=self.user,
                                   mysql_password=self.passwd,unix_socket=self.socket,auto_commit=False).Init()
        self.cur = self.connection.cursor()
        if binlog:
            pass
        else:
            self.cur.execute('set sql_log_bin=0;')
        self.apply_conn = None

        self.insert_sql_list = []

    def CreateTmp(self):
        self.__raise('CREATE DATABASE IF NOT EXISTS dump2db;')                                                                      #创建临时库
        #self.__raise('DROP TABLE IF EXISTS dump2db.dump_status;')
        self.__raise('CREATE TABLE IF NOT EXISTS dump2db.dump_status(id INT,excute_gtid json,logname VARCHAR(100),at_pos BIGINT,next_pos BIGINT,PRIMARY KEY(id));')    #创建临时表

    def SaveStatus(self,logname,at_pos,next_pos,server_id,gtid=None,apply_conn=None):
        if apply_conn:
            return self.__xa_transaction(apply_conn,server_id,logname,at_pos,next_pos,gtid)
        else:
            if gtid:
                sql = 'INSERT INTO dump2db.dump_status(id,excute_gtid,logname,at_pos,next_pos) VALUES(%s,%s,%s,%s,%s)' \
                      ' ON DUPLICATE KEY UPDATE excute_gtid=%s,logname=%s,at_pos=%s,next_pos=%s;'
                self.__raise(sql, (server_id, gtid, logname, at_pos, next_pos, gtid, logname, at_pos, next_pos))
                return self.__raise('commit')
            else:
                sql = 'INSERT INTO dump2db.dump_status(id,logname,at_pos,next_pos) VALUES(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE logname=%s,at_pos=%s,next_pos=%s;'
                self.__raise(sql,(server_id,logname,at_pos,next_pos,logname,at_pos,next_pos))
                return self.__raise('commit')


    def close(self):
        self.cur.close()
        self.connection.close()

    def __raise(self,sql,args=None):
        '''
        一直尝试重新执行，直到成功或超过递归上限
        如遇语法错误将直接退出整个程序
        :param sql:
        :return:
        '''
        try:
            if sql == 'commit':
                self.connection.commit()
            else:
                self.cur.execute(sql,args)
            return True
        except pymysql.IntegrityError:
            Logging(msg=traceback.format_exc(),level='error')
            sys.exit()
        except pymysql.InternalError:
            Logging(msg=traceback.format_exc(), level='error')
            sys.exit()
        except pymysql.Error:
            try:
                state = self.__retry_conn()
                if state:
                    if sql == 'commit':
                        self.__raise(self.insert_sql_list[0],self.insert_sql_list[1])
                        self.__raise('commit')
                    else:
                        self.__raise(sql,args)
                    return True
            except:
                Logging(msg=traceback.format_exc(),level='error')
        self.__raise(sql,args)




    def __retry_conn(self):
        '''
        尝试重连60次，每次间隔1秒
        :return:
        '''
        for i in range(60):
            Logging(msg='retry connection for status db again!!',level='error')
            try:
                self.connection = InitMyDB(mysql_host=self.host, mysql_port=self.port, mysql_user=self.user,
                                           mysql_password=self.passwd, unix_socket=self.socket,auto_commit=False).Init()
                self.cur = self.connection.cursor()
                if self.binlog:
                    pass
                else:
                    self.cur.execute('set sql_log_bin=0;')
                return True
            except pymysql.Error:
                Logging(msg=traceback.format_exc(),level='error')
            time.sleep(1)
        else:
            Logging(msg='retry 60 time on destination db is failed,exist thread now',level='error')
            return False


    def __xa_transaction(self,apply_conn,server_id,logname,at_pos,next_pos,gtid=None):
        '''
        目标库数据提交和状态值在不同库上，完全保证都能提交很难，在次使用数据先提交，确保数据进去之后再
        保存状态数据，控制所有表都有唯一约束或合适的主键的情况下，即使状态丢失也只会在丢失插入数据的事
        务时产生报错，如果先提交状态值在数据提交时断开，这样在其他同步节点接手任务后会遗漏数据
        :param apply_conn:
        :param server_id:
        :param logname:
        :param at_pos:
        :param next_pos:
        :param gtid:
        :return:
        '''
        self.apply_conn = apply_conn
        if gtid:
            sql = 'INSERT INTO dump2db.dump_status(id,excute_gtid,logname,at_pos,next_pos) VALUES(%s,%s,%s,%s,%s)' \
                  ' ON DUPLICATE KEY UPDATE excute_gtid=%s,logname=%s,at_pos=%s,next_pos=%s;'
            self.insert_sql_list = [sql, [server_id, gtid, logname, at_pos, next_pos, gtid, logname, at_pos, next_pos]]
            self.__raise(sql, (server_id, gtid, logname, at_pos, next_pos, gtid, logname, at_pos, next_pos))
        else:
            sql = 'INSERT INTO dump2db.dump_status(id,logname,at_pos,next_pos) VALUES(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE logname=%s,at_pos=%s,next_pos=%s;'
            self.insert_sql_list =  [sql, [server_id, logname, at_pos, next_pos, logname, at_pos, next_pos]]
            self.__raise(sql, (server_id, logname, at_pos, next_pos, logname, at_pos, next_pos))
        try:
            apply_conn.commit()
            self.__raise('commit')
            return True
        except:
            Logging(msg=traceback.format_exc(),level='error')
            return False

