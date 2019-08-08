# -*- coding: utf-8 -*-
''' 
@Time    : 2018/7/21 13:20
@Author  : xiao cai niao
@File    : destination.py
@Software: PyCharm
'''
import sys
import time,uuid
sys.path.append("..")
from lib.Loging import Logging
from lib.escape import escape
from binlog.PrepareStructure import GetStruct
import threading,queue
des_queue = queue.Queue(1024)
error_queue = queue.Queue(10)

thread_lock = threading.Lock()      #控制单个表并发执行的锁
thread_lock_queue = {}              #获取到锁之后执行顺序控制，{'db:tbl':[thread_id,thread_id],....}

chunk_list_status = []              #用于在目标库分块执行时记录每个块执行情况，按入队列顺序报存，当该小块执行完时记录binlog同步状态
                                    #格式[['uuid',length,{status}],......]
chunk_list_status_th = {}           #用于多线程之间同步分块执行情况，这两个变量主要用于binlog状态保存，只有当全部执行才记录，用于宕
                                    #机切换时同步一致性控制，格式{'uuid':[thread_id,thread_id...],....}

table_struct_list = {}  # 字段名列表
table_pk_idex_list = {}  # 主键索引列表

class destination(escape):
    def __init__(self, **kwargs):
        super(destination, self).__init__()

        self.destnation_type = kwargs['destnation_type'] if 'destnation_type' in kwargs else None
        self.queue = kwargs['queue']
        #print('ds : {}'.format(self.queue))
        self.threads = kwargs['dthreads']
        self.save_append_status = {}

        self.server_id = kwargs['server_id']

        self.shost = kwargs['shost']
        self.sport = kwargs['sport']
        self.suser = kwargs['suser']
        self.spassword = kwargs['spassword']
        self.sbinlog = kwargs['sbinlog']

        self.error_queue = None
        self.kwargs = kwargs.copy()
        self.kwargs['queue'] = des_queue
        self.kwargs['error_queue'] = error_queue

        self.th_list = []
        self.__init_status_conn()

    def __init_status_conn(self):
        self._status_conn = GetStruct(host=self.shost, port=self.sport, user=self.suser, passwd=self.spassword,
                                      binlog=self.sbinlog)  # 状态库链接类初始化
        self._status_conn.CreateTmp()

    def __check_stat(self, state,only_state=None):
        if state:
            if only_state:
                return
        else:
            Logging(msg='consume queue is full ,exist now!!!!', level='error')
            if self.error_queue:
                self.error_queue.put(1)
            sys.exit()

    def __get_struct(self):
        global table_struct_list,table_pk_idex_list
        while 1:
            if not self.queue.empty():
                value = self.queue.get()
                if 'table_struct' in value:
                    table_struct_list = value['table_struct'][0]
                    table_pk_idex_list = value['table_struct'][1]
                    break

    def __enter__(self):
        '''
        循环获取队列数据
        :return:
        '''
        self.__get_struct()

        #目标库并发线程初始化
        for i in range(self.threads):
            p = ThreadDump(**dict(self.kwargs,**{'thread_id':i+1}))
            if p:
                p.start()
                self.th_list.append(p)
            else:
                sys.exit()

        #状态库线程初始化
        p = ThreadDump(**{'server_id':self.server_id,'save_status':self._status_conn})
        p.start()
        self.th_list.append(p)


        group_sql = {}
        tmp_status = {}
        num = 0
        interval = int(time.time())

        while 1:
            if not self.queue.empty():
                trancaction = self.queue.get()
                if 'table_struct' in trancaction:
                    global table_struct_list,table_pk_idex_list
                    table_struct_list = trancaction['table_struct'][0]
                    table_pk_idex_list = trancaction['table_struct'][1]
                    continue
                value_list = trancaction['value_list']

                for _value in value_list:
                    db_name,tbl_name = _value[1],_value[2]
                    if '{}:{}'.format(db_name,tbl_name) in group_sql:
                        group_sql['{}:{}'.format(db_name,tbl_name)].append({'gtid':trancaction['gtid'],
                                                                       'gno_uid':trancaction['gno_uid'],
                                                                       'gno_id':trancaction['gno_id'],
                                                                       'binlog':trancaction['binlog'],
                                                                       'at_pos':trancaction['at_pos'],
                                                                       'next_pos':trancaction['next_pos'],
                                                                       'value_list':_value}
                        )
                    else:
                        group_sql['{}:{}'.format(db_name, tbl_name)]=[{'gtid':trancaction['gtid'],
                                                                       'gno_uid':trancaction['gno_uid'],
                                                                       'gno_id':trancaction['gno_id'],
                                                                       'binlog':trancaction['binlog'],
                                                                       'at_pos':trancaction['at_pos'],
                                                                       'next_pos':trancaction['next_pos'],
                                                                       'value_list':_value}
                        ]

                '''100个事务或者10s提交一次到并发序列'''
                num += 1
                tmp_status = {'binlog': trancaction['binlog'], 'at_pos': trancaction['at_pos'],
                              'next_pos': trancaction['next_pos'], 'gtid': trancaction['gtid'],
                              'gno_uid': trancaction['gno_uid']}
                # if (num >= 100 or (int(time.time()) - interval) >= 10):
                #     if error_queue.empty():
                #         tmp_status = {'binlog':trancaction['binlog'],'at_pos':trancaction['at_pos'],
                #                       'next_pos':trancaction['next_pos'],'gtid':trancaction['gtid'],
                #                       'gno_uid':trancaction['gno_uid']}
                #         self.__check_stat(self.__put_queue(value=group_sql,tmp_status=tmp_status),only_state=True)
                #
                #     else:
                #         for th in self.th_list:
                #             th.isDaemon()
                #         Logging(msg='an exception occurred in the inbound thread on destination db...',level='error')
                #         sys.exit()
                if self.__put(group_sql=group_sql,tmp_status=tmp_status,num=num,interval=interval):
                    interval = int(time.time())
                    num = 0
                    group_sql = {}
                continue
            else:
                if self.__put(group_sql=group_sql,tmp_status=tmp_status,num=num,interval=interval):
                    interval = int(time.time())
                    num = 0
                    group_sql = {}
                time.sleep(0.001)

    def __put(self,group_sql,tmp_status,num=None,interval=None):
        if (num >= 100 or (int(time.time()) - interval) >= 10) and group_sql:
            if error_queue.empty():
                self.__check_stat(self.__put_queue(value=group_sql, tmp_status=tmp_status), only_state=True)
                return True
            else:
                for th in self.th_list:
                    th.isDaemon()
                Logging(msg='an exception occurred in the inbound thread on destination db...', level='error')
                sys.exit()
        return False

    def __put_queue(self,value,tmp_status={}):
        '''
        先检测并发队列是否已满，超时时间为60秒，如果超过60秒检测时间都为full
        表示线程已崩，防止在此进入死循环而无法检测目标库线程是否已停止
        :param value:
        :return:
        '''
        for i in range(60):
            if self.__check_queue():
                _uuid = int(uuid.uuid1())
                chunk_list_status.append([_uuid, len(value), tmp_status])
                chunk_list_status_th[_uuid] = []
                for i in value:
                    des_queue.put([i,value[i],_uuid])
                return True
            time.sleep(1)
        else:
            return False

    def __check_queue(self):
        '''
        检查所有队列是否有满的,如果有一个满的表示可能阻塞了
        二是为了防止某一个表落后很多
        :return:
        '''
        if des_queue.full():
            return False
        return True

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class ThreadDump(threading.Thread):
    def __init__(self,**kwargs):
        threading.Thread.__init__(self)
        self.kwargs = kwargs

    def run(self):
        '''
        启动监听或者服务线程
        :return:
        '''
        global thread_lock, thread_lock_queue, chunk_list_status_th,chunk_list_status,table_pk_idex_list,table_struct_list
        if 'save_status' in self.kwargs:
            with save_status(_status_conn=self.kwargs['save_status'],server_id=self.kwargs['server_id'],chunk_list_status=chunk_list_status,chunk_list_status_th=chunk_list_status_th):
                pass
        else:
            if self.kwargs['destnation_type'] == 'phoenix':
                from mode.phoenix.DesThreadPhoenix import desthread as desthreadphoenix
                with desthreadphoenix(**dict(self.kwargs,**{'thread_lock':thread_lock,'thread_lock_queue':thread_lock_queue,
                                                     'chunk_list_status_th':chunk_list_status_th,
                                                     'table_struct_list':table_struct_list,
                                                     'table_pk_idex_list':table_pk_idex_list})):
                    pass
            elif self.kwargs['destnation_type'] == 'postgresql':
                from mode.postgresql.DesThreadPG import desthread as desthreadpg
                with desthreadpg(**dict(self.kwargs,**{'thread_lock':thread_lock,'thread_lock_queue':thread_lock_queue,
                                                     'chunk_list_status_th':chunk_list_status_th,
                                                     'table_struct_list':table_struct_list,
                                                     'table_pk_idex_list':table_pk_idex_list})):
                    pass
            else:
                from .DesThread import desthread
                with desthread(**dict(self.kwargs,**{'thread_lock':thread_lock,'thread_lock_queue':thread_lock_queue,
                                                     'chunk_list_status_th':chunk_list_status_th,
                                                     'table_struct_list':table_struct_list,
                                                     'table_pk_idex_list':table_pk_idex_list})):
                    pass


class save_status:
    def __init__(self,_status_conn,server_id,chunk_list_status,chunk_list_status_th):
        '''
        状态保存.....
        :param _status_conn:
        :param server_id:
        :param chunk_list_status:
        :param chunk_list_status_th:
        '''
        self._status_conn = _status_conn
        self.server_id = server_id
        self.chunk_list_status = chunk_list_status
        self.chunk_list_status_th = chunk_list_status_th
    def __enter__(self):
        while 1:
            if self.chunk_list_status:
                _chunk_uuid = self.chunk_list_status[0][0]
                _chunk_length = self.chunk_list_status[0][1]
                _chunk_status = self.chunk_list_status[0][2]
                while 1:
                    if len(self.chunk_list_status_th[_chunk_uuid]) == _chunk_length:
                        state = self._status_conn.SaveStatus(logname=_chunk_status['binlog'],
                                                at_pos=_chunk_status['at_pos'],
                                                server_id=self.server_id,
                                                next_pos=_chunk_status['next_pos'],
                                                gtid=_chunk_status['gtid'],
                                                gno_uid=_chunk_status['gno_uid'],
                                                apply_conn=None)
                        if state:
                            chunk_list_status.pop(0)
                            del chunk_list_status_th[_chunk_uuid]
                            break
                        else:
                            sys.exit()
                    time.sleep(0.001)
            time.sleep(0.001)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass