# -*- coding: utf-8 -*-
''' 
@Time    : 2018/7/25 15:47
@Author  : Great God
@File    : DesThreadPhoenix.py
@Software: PyCharm
'''

import sys,pymysql,traceback,time
sys.path.append("..")
from lib.ErrorCode import ErrorCode
from lib.ErrorCode import RetryErrorCode
from lib.Loging import Logging
from lib.InitDB import InitMyDB
from .GetSql import GetSql

class desthread(GetSql):
    def __init__(self, **kwargs):
        super(desthread, self).__init__(table_pk_idex_list=kwargs['table_pk_idex_list'],
                                        table_struct_list=kwargs['table_struct_list'],
                                        map_conf=kwargs['map_conf'],
                                        queal_struct=kwargs['queal_struct'])

        self.thread_lock = kwargs['thread_lock']
        self.thread_lock_queue = kwargs['thread_lock_queue']
        self.chunk_list_status_th = kwargs['chunk_list_status_th']
        self.table_struct_list = kwargs['table_struct_list']
        self.table_pk_idex_list = kwargs['table_pk_idex_list']
        self.thread_id = kwargs['thread_id']

        self.queue = kwargs['queue']
        self.dhost, self.dport, self.duser, self.dpasswd = kwargs['dhost'], kwargs['dport'], kwargs['duser'], kwargs['dpassword']  # 目标库连接相关信息
        self.binlog = kwargs['binlog']  # 是否在目标库记录binlog的参数

        self.destination_conn = None
        self.destination_cur = None

        self.trancaction_list = []  # 已执行的事务sql,用于重连之后重新执行
        self.queue_list = []        #队列列表，一个队列对应一个线程,[queue1,queue2]
        self.group_list = []        #存储每个队列分配的db.tbl组合，对应队列列表索引关系[['db:tbl','db:tbl'],['db:tbl']]
        self.status_row = []        #存储每个对应库、表是否已存在于标签表，用于改善insert ....update语句，降低死锁风险

        self.error_queue = kwargs['error_queue']
        self.kwargs = None
        self.__init_slave_conn()


    def __init_slave_conn(self):
        '''
        初始化同步所需的状态库、目标库的链接
        :return:
        '''
        for i in range(60):
            try:
                self.destination_conn = InitMyDB(mysql_host=self.dhost, mysql_port=self.dport, mysql_user=self.duser,
                                                 mysql_password=self.dpasswd, auto_commit=False).Init()
                self.destination_cur = self.destination_conn.cursor()

                if self.binlog is None:
                    self.destination_cur.execute('set sql_log_bin=0;')  # 设置binlog参数
                self.destination_cur.execute('SET SESSION wait_timeout = 2147483;')
                break
            except pymysql.Error as e:
                Logging(msg=e.args,level='error')
            time.sleep(1)
        else:
            Logging(msg='retry 60 time on slave db is failed,exist thread now',level='error')
            self.error_queue.put(1)
            sys.exit()

    def __restart_trancaction_sql(self):
        '''
        在提交时链接断开的情况下需要重新执行所有事务操作
        :return:
        '''
        #self.__retry_connection_destion()
        for row in self.trancaction_list:
            if self.__raise_sql(sql=row[0], args=row[1], tmp=True) is None:
                return None
        return True

    def __raise_sql(self, sql, args=[],retry=None,type=None,tmp=None):
        '''
        追加binlog数据到目标库
        :param sql:
        :param args:
        :param retry:  为True代表如果报错需要重新执行整个事务，用于commit
        :param type:   为True代表报错时需要重新执行sql，用于select
        :param tmp:    为True代表该sql为重试得sql，如果报错直接返回
        :return:
        '''
        try:
            args = self.escape_string(args) if args else []
        except:
            Logging(msg=traceback.format_exc(),level='error')
            self.error_queue.put(1)
        try:
            if sql == 'commit':
                self.destination_conn.commit()
            else:
                self.destination_cur.execute(sql, args)
        except pymysql.Error as e:
            Logging(msg=e.args, level='error')
            if tmp:
                return None
            if e.args[0] in RetryErrorCode:
                Logging(msg='sql: {}  args:{}'.format(sql,args),level='error')
                self.__raise_sql(sql=sql,args=args,retry=retry,type=type)
            elif e.args[0] in ErrorCode:
                self.__rollback()
                if ErrorCode[e.args[0]]:
                    if sql == 'commit':
                        self.__retry_execute(retry=retry)
                    else:
                        self.__retry_execute(sql=sql,args=args,retry=retry,type=type)
                    return True
            Logging(msg='sql:{},values:{}'.format(sql, args), level='error')
            Logging(msg=traceback.format_exc(), level='error')
            return None

        except:
            Logging(msg='sql:{},values:{}'.format(sql, args), level='error')
            Logging(msg=traceback.format_exc(), level='error')
            return None
        return True

    def __rollback(self):
        '''
        关闭链接，用于兼容中间件
        :return:
        '''
        try:
            Logging(msg='close connection for db now !!!',level='warning')
            self.destination_cur.close()
            self.destination_conn.close()
        except pymysql.Error as e:
            Logging(msg=e.args,level='warning')
        except:
            Logging(msg=traceback.format_exc(), level='warning')

    def __retry_execute(self,sql=None,args=None,retry=None,type=None):
        '''
        异常重试
        :param sql: sql语句
        :param args: 参数列表
        :param type: 报错时是否需要重新执行该sql
        :param retry: 需要全事务重新执行
        :return:
        '''
        self.__retry_connection_destion()
        if type:
            self.__raise_sql(sql=sql,args=args,type=type,retry=retry)
            return
        elif retry is None:
            Logging(msg='sql={},args={},retry={},type={}'.format(sql, args,retry, type), level='info')
            Logging(msg='retry execute trancaction list, list length {}'.format(len(self.trancaction_list)),
                    level='info')
            for row in self.trancaction_list:
                Logging(msg='retry sql : {} args:{}'.format(row[0],row[1]),level='warning')
                if self.__raise_sql(row[0], row[1],tmp=True) is None:
                    self.__rollback()
                    self.__retry_execute()
                    return
            return
        elif retry:
            Logging(msg='sql={},args={},retry={},type={}'.format(sql, args, retry, type), level='info')
            Logging(msg='retry all trancaction list, list length {}'.format(len(self.trancaction_list)),
                    level='info')
            if self.__restart_trancaction_sql() is None:
                self.__retry_execute(retry=True)
                self.__raise_sql('commit', retry=True)
                return
            self.__raise_sql('commit',retry=True)
            return



    def __retry_connection_destion(self):
        '''
        目标库链接丢失重试60次，如果60次都失败将退出整个程序
        使用30次的原因是有可能目标数据在发生宕机切换，如果30
        秒都无法完成重连那表示数据库已经宕机或无法链接
        :return:
        '''
        import time
        for i in range(60):
            time.sleep(1)
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
                return True
            except:
                Logging(msg=traceback.format_exc(), level='error')

        else:
            Logging(msg='try 60 times to fail for conncetion destination db,exist now', level='error')
            self.error_queue.put(1)
            sys.exit()

    def __check_stat(self, state,only_state=None):
        if state:
            if only_state:
                return
        else:
            Logging(msg='desthread failed!!!!', level='error')
            self.error_queue.put(1)
            sys.exit()

    def __check_queue(self):
        '''
        检查所有队列是否有满的,如果有一个满的表示可能阻塞了
        二是为了防止某一个表落后很多
        :return:
        '''
        if self.queue.full():
            return False
        return True

    def __set_mark(self,db_name,tbl_name,gtid,gno_id,at_pos,binlog):
        '''
        标签操作、也是状态记录,记得该表执行到的gtid、position位置
        :param db_name:
        :param tbl_name:
        :param gtid:
        :param gno_id:
        :param at_pos:
        :return:
        '''
        _name = '{}:{}'.format(db_name,tbl_name)
        if _name in self.status_row:
            sql = 'UPDATE repl_mark.mark_status SET gno_uid=%s,gno_id=%s,at_pos=%s,binlog=%s where db_name=%s and tbl_name=%s;'
            args = [gtid, gno_id, at_pos,binlog,db_name,tbl_name]
        else:
            sql = 'select 1 from repl_mark.mark_status where  db_name = %s and tbl_name = %s;'
            if self.__raise_sql(sql=sql,args=[db_name,tbl_name],type=True):
                _s = self.destination_cur.fetchall()
                if _s:
                    sql = 'UPDATE repl_mark.mark_status SET gno_uid=%s,gno_id=%s,binlog=%s,at_pos=%s where db_name=%s and tbl_name = %s;'
                    args = [gtid,gno_id,binlog,at_pos,db_name,tbl_name]
                else:
                    sql = 'INSERT INTO repl_mark.mark_status(db_name,tbl_name,gno_uid,gno_id,binlog,at_pos) VALUES(%s,%s,%s,%s,%s,%s);'
                    args = [db_name,tbl_name,gtid,gno_id,binlog,at_pos]
                self.status_row.append('{}:{}'.format(db_name, tbl_name))
            else:
                Logging(msg='execute sql [{}] error , exit now!!!!'.format(sql),level='error')
                self.error_queue.put(1)
                sys.exit()
        self.trancaction_list.append([sql,args])
        self.__check_stat(self.__raise_sql(sql=sql,args=args))

    def __get_fetch_all(self):
        '''
        重起获取所有库、表已执行的游标
        :return:
        '''
        sql = 'SELECT db_name,tbl_name,gno_uid,gno_id,binlog,at_pos FROM repl_mark.mark_status'
        self.__check_stat(self.__raise_sql(sql=sql,type=True))
        result = self.destination_cur.fetchall()
        fetch_value = {}
        for row in result:
            fetch_value['{}:{}'.format(row['db_name'],row['tbl_name'])] = [row['gno_uid'],row['gno_id'],row['at_pos'],row['binlog']]
        return fetch_value

    def __check_lock(self,db_tbl_name,lock_state=None):
        '''
        同一库、表顺序锁
        :param db_tbl_name:
        :return:
        '''

        if db_tbl_name in self.thread_lock_queue:
            if lock_state:
                self.thread_lock.release()
            if self.thread_id in self.thread_lock_queue[db_tbl_name]:
                while 1:
                    if self.thread_lock_queue[db_tbl_name][0] == self.thread_id:
                        return True
                    time.sleep(0.001)
            else:
                self.thread_lock_queue[db_tbl_name].append(self.thread_id)
                self.__check_lock(db_tbl_name=db_tbl_name)
        else:
            if lock_state:
                self.thread_lock_queue[db_tbl_name] = [self.thread_id]
                self.thread_lock.release()
                self.__check_lock(db_tbl_name=db_tbl_name)
            self.thread_lock.acquire()
            self.__check_lock(db_tbl_name=db_tbl_name,lock_state=True)

    def __enter__(self):
        fetch_value = self.__get_fetch_all()
        __mark_status = None
        while 1:
            if not self.queue.empty():
                queue_value = self.queue.get()
                db_tbl_name = queue_value[0]
                trancaction_all = queue_value[1]
                _uuid = queue_value[2]
                if trancaction_all:
                    self.__check_lock(db_tbl_name)
                    for trancaction in trancaction_all:
                        tran_len = len(trancaction_all)
                        value_list = trancaction['value_list']
                        if value_list:
                            db_name,tbl_name = value_list[1],value_list[2]
                            gno_uid,gno_id,binlog,at_pos = trancaction['gno_uid'],trancaction['gno_id'],trancaction['binlog'],value_list[3]

                            if db_tbl_name in fetch_value:
                                _state = self.__fetch_check(fetch_value[db_tbl_name],gno_uid,gno_id,at_pos,binlog)
                                if _state:
                                    del fetch_value[db_tbl_name]
                                else:
                                    continue
                            if tran_len >= 2:
                                if __mark_status is None:
                                    self.__set_mark(db_name=db_name, tbl_name=tbl_name, gtid=gno_uid,gno_id=gno_id, at_pos=at_pos,binlog=binlog)
                                    __mark_status = True
                            else:
                                self.__set_mark(db_name=db_name, tbl_name=tbl_name, gtid=gno_uid, gno_id=gno_id,binlog=binlog,
                                                at_pos=at_pos)

                            sql_list = self.GetSQL(_values=value_list[0],event_code=value_list[4],database_name=db_name,table_name=tbl_name)
                            if sql_list:
                                for sql in sql_list:
                                    self.trancaction_list.append([sql[0], sql[1]])
                                    self.__check_stat(self.__raise_sql(sql=sql[0], args=sql[1]))
                    if __mark_status:
                        self.__set_mark(db_name=db_name, tbl_name=tbl_name, gtid=gno_uid, gno_id=gno_id, at_pos=at_pos,binlog=binlog)
                    self.__raise_sql('commit',retry=True)
                    self.chunk_list_status_th[_uuid].append(self.thread_id)
                    self.trancaction_list = []
                    __mark_status = None
                    self.thread_lock_queue[db_tbl_name].pop(0)


    def __fetch_check(self,fetch,gno_uid,gno_id,at_pos,binlog):
        '''
        重启或接管任务时通过游标库中相应库表保存的同步状态信息，比对获取到binlog的信息
        如果游标库中的uid和获取到的binlog中不同，这种情况可能是由于mysql宕机切换导致，
        由于游标库先于状态库落盘，所以以游标库为准
        :param fetch:  [gno_uid,gno_id,at_pos]
        :param gno_uid:
        :param gno_id:
        :param at_pos:
        :return:
        '''
        if gno_uid:
            if str(gno_uid) == str(fetch[0]):
                if  int(fetch[1]) > int(gno_id):
                    return False
                elif int(gno_id) == int(fetch[1]) and int(fetch[2]) >= int(at_pos):
                    return False
                else:
                    return True
            elif str(gno_uid) != str(fetch[0]):
                return False
        else:
            if fetch[-1] == binlog:
                if int(fetch[2] >= int(at_pos)):
                    return False
                else:
                    return True
            else:
                binlog_num_cur = int(fetch[-1].split('.')[-1])
                binlog_num_sync = int(binlog.split('.')[-1])
                if binlog_num_cur < binlog_num_sync:
                    return False
                else:
                    return True


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.destination_cur.close()
        self.destination_conn.close()