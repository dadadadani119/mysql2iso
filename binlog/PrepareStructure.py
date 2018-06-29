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
    def __init__(self,host=None,port=None,user=None,passwd=None,socket=None):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.socket = socket

        self.connection = InitMyDB(mysql_host=self.host,mysql_port=self.port,mysql_user=self.user,
                                   mysql_password=self.passwd,unix_socket=self.socket,auto_commit=True).Init()
        self.cur = self.connection.cursor()
        self.cur.execute('set sql_log_bin=0;')
        self.apply_conn = None

    def GetColumn(self,*args):
        '''args顺序 database、tablename'''
        column_list = []
        column_type_list = []

        sql = 'select COLUMN_NAME,COLUMN_KEY,COLUMN_TYPE from INFORMATION_SCHEMA.COLUMNS where table_schema=%s and table_name=%s order by ORDINAL_POSITION;'
        self.__raise(sql,args=args)
        result = self.cur.fetchall()
        pk_idex = []
        for idex,row in enumerate(result):
            column_list.append(row['COLUMN_NAME'])
            column_type_list.append(row['COLUMN_TYPE'])
            if row['COLUMN_KEY'] == 'PRI':
                pk_idex.append(idex)
        return column_list,pk_idex,column_type_list

    def CreateTmp(self):
        self.__raise('CREATE DATABASE IF NOT EXISTS dump2db;')                                                                      #创建临时库
        #self.__raise('DROP TABLE IF EXISTS dump2db.dump_status;')
        self.__raise('CREATE TABLE IF NOT EXISTS dump2db.dump_status(id INT,excute_gtid json,logname VARCHAR(100),at_pos BIGINT,next_pos BIGINT,PRIMARY KEY(id));')    #创建临时表

    def SaveStatus(self,logname,at_pos,next_pos,server_id,gtid=None,apply_conn=None,xa=None):
        if xa:
            self.__xa_transaction(apply_conn,server_id,logname,at_pos,next_pos,gtid)
        else:
            if gtid:
                sql = 'INSERT INTO dump2db.dump_status(id,excute_gtid,logname,at_pos,next_pos) VALUES(%s,%s,%s,%s,%s)' \
                      ' ON DUPLICATE KEY UPDATE excute_gtid=%s,logname=%s,at_pos=%s,next_pos=%s;'
                self.__raise(sql, (server_id, gtid, logname, at_pos, next_pos, gtid, logname, at_pos, next_pos))
            else:
                sql = 'INSERT INTO dump2db.dump_status(id,logname,at_pos,next_pos) VALUES(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE logname=%s,at_pos=%s,next_pos=%s;'
                self.__raise(sql,(server_id,logname,at_pos,next_pos,logname,at_pos,next_pos))



    def close(self):
        self.cur.close()
        self.connection.close()

    def __raise(self,sql,args=None):
        '''
        一直尝试重新执行，知道成功或超过递归上限
        如遇语法错误将直接退出整个程序
        :param sql:
        :return:
        '''
        try:
            self.cur.execute(sql,args)
            return
        except pymysql.IntegrityError:
            Logging(msg=traceback.format_list(),level='error')
            self.cur.execute('XA ROLLBACK "save_state"')
            self.apply_conn.execute('XA ROLLBACK "apply"')
            sys.exit()
        except pymysql.InternalError:
            Logging(msg=traceback.format_list(), level='error')
            self.cur.execute('XA ROLLBACK "save_state"')
            self.apply_conn.execute('XA ROLLBACK "apply"')
            sys.exit()
        except pymysql.Error:
            state = self.__retry_conn()
            if state:
                self.cur.execute(sql,args)
                return
        self.__raise(sql,args)




    def __retry_conn(self):
        '''
        尝试重连5次，每次间隔1秒
        :return:
        '''
        for i in range(5):
            try:
                self.connection = InitMyDB(mysql_host=self.host, mysql_port=self.port, mysql_user=self.user,
                                           mysql_password=self.passwd, unix_socket=self.socket).Init()
                self.cur = self.connection.cursor()
                self.cur.execute('set sql_log_bin=0;')
                try:
                    self.cur.execute('XA ROLLBAK "save_state"')
                except:
                    self.cur.execute('XA END "save_state"')
                    self.cur.execute('XA ROLLBAK "save_state"')
                return True
            except pymysql.Error:
                Logging(msg=traceback.format_list(),level='error')
            time.sleep(1)
        else:
            Logging(msg='retry 5 time on destination db is failed,exist thread now',level='error')
            return False


    def __xa_transaction(self,apply_conn,server_id,logname,at_pos,next_pos,gtid=None):
        self.apply_conn = apply_conn
        self.__check_xaid()
        if gtid:
            sql = 'INSERT INTO dump2db.dump_status(id,excute_gtid,logname,at_pos,next_pos) VALUES(%s,%s,%s,%s,%s)' \
                  ' ON DUPLICATE KEY UPDATE excute_gtid=%s,logname=%s,at_pos=%s,next_pos=%s;'
            self.__raise(sql, (server_id, gtid, logname, at_pos, next_pos, gtid, logname, at_pos, next_pos))
        else:
            sql = 'INSERT INTO dump2db.dump_status(id,logname,at_pos,next_pos) VALUES(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE logname=%s,at_pos=%s,next_pos=%s;'
            self.__raise(sql, (server_id, logname, at_pos, next_pos, logname, at_pos, next_pos))
        self.cur.execute('XA END "save_state"')
        self.cur.execute('XA PREPARE "save_state"')
        try:
            self.cur.execute('XA COMMIT "save_state"')
            apply_conn.execute('XA COMMIT "apply"')
        except:
            self.cur.execute('XA ROLLBACK "save_state"')
            apply_conn.execute('XA ROLLBACK "apply"')

    def __check_xaid(self):
        '''
        检查是否存在同名的xa事务，如果存在直接回滚释放
        :return:
        '''
        try:
            self.cur.execute('XA start "save_state"')
        except pymysql.InternalError:
            self.cur.execute('xa recover')
            result = self.cur.fetchall()
            if result:
                for xa in result:
                    if xa['data'] == 'save_state':
                        self.cur.execute('XA ROLLBACK "save_state"')
            self.cur.execute('XA start "save_state"')
