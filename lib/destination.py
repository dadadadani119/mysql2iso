# -*- coding: utf-8 -*-
''' 
@Time    : 2018/7/21 13:20
@Author  : Great God
@File    : destination.py
@Software: PyCharm
'''
import sys,pymysql,traceback
import time
from .ErrorCode import ErrorCode
from .Loging import Logging
from .InitDB import InitMyDB
from .escape import escape
sys.path.append("..")
from binlog.PrepareStructure import GetStruct


class destination(escape):
    def __init__(self, **kwargs):
        super(destination, self).__init__()
        self.queue = kwargs['queue']
        print('ds : {}'.format(self.queue))
        self.dhost, self.dport, self.duser, self.dpasswd = kwargs['dhost'], kwargs['dport'], kwargs['duser'], kwargs['dpassword']  # 目标库连接相关信息
        self.binlog = kwargs['binlog']  # 是否在目标库记录binlog的参数

        self.destination_conn = None
        self.destination_cur = None

        self.trancaction_list = []  # 已执行的事务sql,用于重连之后重新执行
        self._status_conn = None

        self.server_id = kwargs['server_id']

        self.shost = kwargs['shost']
        self.sport = kwargs['sport']
        self.suser = kwargs['suser']
        self.spassword = kwargs['spassword']
        self.sbinlog = kwargs['sbinlog']

        self.__init_slave_conn()

    def __init_slave_conn(self):
        '''
        初始化同步所需的状态库、目标库的链接
        :return:
        '''
        self.destination_conn = InitMyDB(mysql_host=self.dhost, mysql_port=self.dport, mysql_user=self.duser,
                                         mysql_password=self.dpasswd, auto_commit=False).Init()
        self.destination_cur = self.destination_conn.cursor()

        if self.binlog is None:
            self.destination_cur.execute('set sql_log_bin=0;')  # 设置binlog参数
        self.destination_cur.execute('SET SESSION wait_timeout = 2147483;')

        self._status_conn = GetStruct(host=self.shost, port=self.sport, user=self.suser, passwd=self.spassword,
                                      binlog=self.sbinlog)  # 状态库链接类初始化
        self._status_conn.CreateTmp()

    def __save_status(self, binlog_file_name, at_pos, next_pos, save_gtid_value, _apply_conn, gno_uid):
        '''
        保存同步状态值及提交数据修改
        :param binlog_file_name:
        :param at_pos:
        :param next_pos:
        :param save_gtid_value:
        :param _apply_conn:
        :return:
        '''
        state = self._status_conn.SaveStatus(logname=binlog_file_name, at_pos=at_pos, next_pos=next_pos,
                                             server_id=self.server_id,
                                             gtid=save_gtid_value,
                                             apply_conn=_apply_conn,
                                             gno_uid=gno_uid)
        if state:
            return True
        else:
            Logging(msg='execute trancaction all sql try again', level='error')
            self.__restart_trancaction_sql()
            self.__save_status(binlog_file_name, at_pos, next_pos, save_gtid_value, _apply_conn)

    def __restart_trancaction_sql(self):
        '''
        在提交时链接断开的情况下需要重新执行所有事务操作
        :return:
        '''
        self.__retry_connection_destion()
        for row in self.trancaction_list:
            self.__check_stat(self.__raise_sql(sql=row[0], args=row[1], retry=True))
        #self.__check_stat(self.__raise_sql('commit',retry=True))
        return

    def __raise_sql(self, sql, args=[],retry=None):
        '''
        追加binlog数据到目标库
        :param sql:
        :param args:
        :return:
        '''
        args = self.escape_string(args) if args else []
        try:
            if sql == 'commit':
                self.destination_conn.commit()
            else:
                self.destination_cur.execute(sql, args)
        except pymysql.Error as e:
            Logging(msg=traceback.format_exc(), level='error')
            if ErrorCode[e.args[0]]:
                self.__retry_execute(retry=retry)
            else:
                Logging(msg='sql:{},values:{}'.format(sql, args), level='error')
                Logging(msg=e, level='error')
                return None
        except:
            Logging(msg=traceback.format_exc(), level='error')
            return None
        return True

    def __retry_execute(self,retry=None):
        '''
        异常重试
        :param sql: sql语句
        :param args: 参数列表
        :param type: 是否需要重新执行该sql
        :param retry: 是否是重新执行的sql
        :return:
        '''
        self.__retry_connection_destion()
        if retry is None:
            # for row in tmepdata.sql_list:
            Logging(msg='retry execute trancaction list, list length {}'.format(len(self.trancaction_list)),
                    level='info')
            for row in self.trancaction_list:
                self.__raise_sql(row[0], row[1])
            return
                # self.destination_cur.execute(row[0], row[1])
        elif retry:
            self.__restart_trancaction_sql()


    def __retry_connection_destion(self):
        '''
        目标库链接丢失重试60次，如果60次都失败将退出整个程序
        使用30次的原因是有可能目标数据在发生宕机切换，如果30
        秒都无法完成重连那表示数据库已经宕机或无法链接
        :return:
        '''
        import time
        for i in range(60):
            Logging(msg='connection to destination db try agian!!!', level='info')
            try:
                self.destination_conn = InitMyDB(mysql_host=self.dhost, mysql_port=self.dport,
                                                 mysql_user=self.duser,
                                                 mysql_password=self.dpasswd, auto_commit=False).Init()
                self.destination_cur = self.destination_conn.cursor()
                Logging(msg='connection success!!!', level='info')
                if self.binlog is None:
                    self.destination_cur.execute('set sql_log_bin=0;')  # 设置binlog参数
                self.destination_cur.execute('SET SESSION wait_timeout = 2147483;')
                self.__set_mark()
                return True
            except:
                Logging(msg=traceback.format_exc(), level='error')
            time.sleep(1)
        else:
            Logging(msg='try 60 times to fail for conncetion destination db,exist now', level='error')
            sys.exit()

    def __check_stat(self, state):
        if state:
            pass
        else:
            Logging(msg='failed!!!!', level='error')
            sys.exit()

    def __enter__(self):
        '''
        循环获取队列数据并执行
        :return:
        '''
        while 1:
            print(self.queue.qsize())
            if not self.queue.empty():
                trancaction = self.queue.get()
                sql_list = trancaction['sql_list']
                if sql_list:
                    for sql in sql_list:
                        self.trancaction_list.append([sql[0], sql[1]])
                        self.__check_stat(self.__raise_sql(sql=sql[0], args=sql[1]))
                    self.__save_status(binlog_file_name=trancaction['binlog'], at_pos=trancaction['at_pos'],
                                       next_pos=trancaction['next_pos'], save_gtid_value=trancaction['gtid'],
                                       _apply_conn=self.destination_conn, gno_uid=trancaction['gno_uid'])
                else:
                    self.__save_status(binlog_file_name=trancaction['binlog'], at_pos=trancaction['at_pos'],
                                       next_pos=trancaction['next_pos'], save_gtid_value=trancaction['gtid'],
                                       _apply_conn=None, gno_uid=trancaction['gno_uid'])
                self.trancaction_list = []
                continue
            else:
                time.sleep(1)
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.destination_cur.close()
        self.destination_conn.close()