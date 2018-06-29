# -*- encoding: utf-8 -*-
'''
@author: Great God
'''
import sys,pymysql,traceback
import json
from .Loging import Logging
from .InitDB import InitMyDB
sys.path.append("..")
from dump.processdump import processdump
from binlog.Replication import ReplicationMysql
from binlog.ParseEvent import ParseEvent
from binlog.PrepareStructure import GetStruct
from binlog.Metadata import binlog_events

class tmepdata:
    database_name,table_name,cloums_type_id_list,metadata_dict = None,None,None,None

    #下面三个记录类型都以'db:tab' 做key,对应数据做value
    table_struct_list = {}              #字段名列表
    table_pk_idex_list = {}             #主键索引列表
    table_struct_type_list = {}         #字段类型列表

    unsigned_list = {}
    thread_id = None
    excute_gtid = {}
<<<<<<< HEAD
    sql_list = []                       #记录一个binlog事务的所有sql语句

    repl_mark = None
=======
>>>>>>> 4ddcef19edcd96b31c1403fa3e2e920a9ecd5117

class OperationDB:
    def __init__(self,**kwargs):
        self.ssl_auth = {'cert':kwargs['cert'],'key':kwargs['key']} if kwargs['ssl'] else None
        self.daemon = kwargs['daemon']

        self.full_dump = kwargs['full_dump']                                                                                                        #是否全量导出
        self.threads = kwargs['threads']                                                                                                            #全量导出时并发线程

        self.host,self.port,self.user,self.passwd = kwargs['host'],kwargs['port'],kwargs['user'],kwargs['passwd']                                   #源库连接相关信息
        self.unix_socket = kwargs['socket']

        self.dhost,self.dport,self.duser,self.dpasswd = kwargs['dhost'],kwargs['dport'],kwargs['duser'],kwargs['dpasswd']                           #目标库连接相关信息
        self.binlog = kwargs['binlog']                                                                                                              #是否在目标库记录binlog的参数

        self.destination_conn = None
        self.destination_cur = None
        self.conn = None

        self.repl_mark = None
        self.xa = None
        self.apply_conn = None
        self.crc = None

        self.databases = kwargs['databases']
        self.tables = kwargs['tables']
        self.binlog_file = kwargs['binlog_file']
        self.start_position = kwargs['start_position']
        self.auto_position = kwargs['auto_position']
        self.gtid = kwargs['gtid']

        self.ithread = kwargs['ithread']
        self.ignore_type = kwargs['ignore_type']
        self.ignore = {'delete':binlog_events.DELETE_ROWS_EVENT,'update':binlog_events.UPDATE_ROWS_EVENT,'insert':binlog_events.WRITE_ROWS_EVENT}
        self.server_id = kwargs['server_id']

    def __init_master_slave_conn(self):
        '''
        初始化同步所需的源库、目标库的链接
        :return:
        '''
        self.destination_conn = InitMyDB(mysql_host=self.dhost, mysql_port=self.dport, mysql_user=self.duser,
                                         mysql_password=self.dpasswd,auto_commit=True).Init()

        self.destination_cur = self.destination_conn.cursor()

        if self.binlog is None:
            self.destination_cur.execute('set sql_log_bin=0;')  # 设置binlog参数
        self.destination_cur.execute('SET SESSION wait_timeout = 2147483;')

        self.conn = InitMyDB(mysql_host=self.host, mysql_port=self.port, mysql_user=self.user,
                             mysql_password=self.passwd, unix_socket=self.unix_socket,ssl=self.ssl_auth).Init()

    def WhereJoin(self,table_struce_key):
        return ' AND '.join(['`{}`=%s'.format(col) for col in tmepdata.table_struct_list[table_struce_key]])

    def SetJoin(self,table_struce_key):
        return ','.join(['`{}`=%s'.format(col) for col in tmepdata.table_struct_list[table_struce_key]])

    def ValueJoin(self,table_struce_key):
        return '({})'.format(','.join(['%s' for i in range(len(tmepdata.table_struct_list[table_struce_key]))]))

    def PkJoin(self,pk_list,table_struce_key):
        pk_col = []
        for pk in pk_list:
            pk_col.append(tmepdata.table_struct_list[table_struce_key][pk])
        if len(pk_col) > 1:
            return ' AND '.join(['`{}`=%s'.format(col) for col in pk_col])
        else:
            return '`{}`=%s'.format(pk_col[0])

    def GetSQL(self,_values=None,event_code=None):
        table_struce_key = '{}:{}'.format(tmepdata.database_name,tmepdata.table_name)

        if tmepdata.table_pk_idex_list[table_struce_key]:
            '''获取主键所在index'''
            __pk_idx = tmepdata.table_pk_idex_list[table_struce_key]
            pk_where = self.PkJoin(__pk_idx,table_struce_key)
        else:
            __pk_idx = None

        if event_code == binlog_events.UPDATE_ROWS_EVENT:
            __values = [_values[i:i + 2] for i in range(0, len(_values), 2)]
            for row_value in __values:
                if __pk_idx is not None:
                    cur_sql = 'UPDATE {}.{} SET {} WHERE {}'.format(tmepdata.database_name, tmepdata.table_name,
                                                                       self.SetJoin(table_struce_key), pk_where)
                    pk_values = []
                    for i in __pk_idx:
                        pk_values.append(row_value[0][i])
                    _args = row_value[1] + pk_values
                else:
                    cur_sql = 'UPDATE {}.{} SET {} WHERE {}'.format(tmepdata.database_name, tmepdata.table_name,
                                                                   self.SetJoin(table_struce_key),
                                                                   self.WhereJoin(table_struce_key))
                    _args = row_value[1] + row_value[0]

                tmepdata.sql_list.append([cur_sql,_args])
                self.__check_stat(self.__put_new_db(cur_sql,args=_args))

        else:
            if event_code == binlog_events.WRITE_ROWS_EVENT:
                values_str = ','.join([self.ValueJoin(table_struce_key) for i in range(len(_values))])
                cur_sql = 'INSERT INTO {}.{} VALUES{};'.format(tmepdata.database_name, tmepdata.table_name,
                                                                   values_str)
                all_values = []
                for value in _values:
                    all_values += value
                self.__check_stat(self.__put_new_db(cur_sql,all_values))
            elif event_code == binlog_events.DELETE_ROWS_EVENT:
                for value in _values:
                    if __pk_idx is not None:
                        cur_sql = 'DELETE FROM {}.{} WHERE {};'.format(tmepdata.database_name,tmepdata.table_name,pk_where)
                        pk_values = []
                        for i in __pk_idx:
                            pk_values.append(value[i])
                        _args = pk_values
                    else:
                        cur_sql = 'DELETE FROM {}.{} WHERE {};'.format(tmepdata.database_name,tmepdata.table_name,self.WhereJoin(table_struce_key))
                        _args = value
                    tmepdata.sql_list.append([cur_sql,_args])
                    self.__check_stat(self.__put_new_db(cur_sql,_args))
            '''
            for value in _values:
                if event_code == binlog_events.WRITE_ROWS_EVENT:
                    cur_sql = 'INSERT INTO {}.{} VALUES{};'.format(tmepdata.database_name, tmepdata.table_name,
                                                                   self.ValueJoin(table_struce_key))
                    _args = value

                elif event_code == binlog_events.DELETE_ROWS_EVENT:
                    if __pk_idx is not None:
                        cur_sql = 'DELETE FROM {}.{} WHERE {};'.format(tmepdata.database_name,tmepdata.table_name,pk_where)
                        pk_values = []
                        for i in __pk_idx:
                            pk_values.append(value[i])
                        _args = pk_values
                    else:
                        cur_sql = 'DELETE FROM {}.{} WHERE {};'.format(tmepdata.database_name,tmepdata.table_name,self.WhereJoin(table_struce_key))
                        _args = value
                state = self.__put_new_db(cur_sql,args=_args)
<<<<<<< HEAD
            '''
=======

        if state:
            pass
        else:
            self.destination_conn.rollback()
            Logging(msg='failed!!!!', level='error')
            sys.exit()
>>>>>>> 4ddcef19edcd96b31c1403fa3e2e920a9ecd5117

    def __execute_code(self,_parse_event,event_code,event_length,table_struce_key):
        if tmepdata.database_name and tmepdata.table_name and tmepdata.database_name in self.databases:
            if self.tables:
                if tmepdata.table_name in self.tables:
                    _values = _parse_event.GetValue(type_code=event_code, event_length=event_length,
                                                    cloums_type_id_list=tmepdata.cloums_type_id_list,
                                                    metadata_dict=tmepdata.metadata_dict,
                                                    unsigned_list=tmepdata.table_struct_type_list[table_struce_key])
                    self.GetSQL(_values=_values, event_code=event_code)
            else:
                _values = _parse_event.GetValue(type_code=event_code, event_length=event_length,
                                                cloums_type_id_list=tmepdata.cloums_type_id_list,
                                                metadata_dict=tmepdata.metadata_dict,
                                                unsigned_list=tmepdata.table_struct_type_list[table_struce_key])
                self.GetSQL(_values=_values, event_code=event_code)

    def Operation(self):
        '''
        :return:
        '''

        '''全量导出入口'''
        if self.daemon:
            pass
        elif self.full_dump:
            des_mysql_info = {'mysql_host':self.dhost,'mysql_port':self.dport,'mysql_user':self.duser,
                              'mysql_password':self.dpasswd}
            src_mysql_info = {'mysql_host':self.host,'mysql_port':self.port,'mysql_user':self.user,
<<<<<<< HEAD
                              'mysql_password':self.passwd,'unix_socket':self.unix_socket,'ssl':self.ssl_auth}
=======
                              'mysql_password':self.passwd,'unix_socket':self.unix_socket}
>>>>>>> 4ddcef19edcd96b31c1403fa3e2e920a9ecd5117
            _binlog_file,_binlog_pos,_excute_gtid = processdump(threads=self.threads,dbs=self.databases,tables=self.tables,
                                                   src_kwargs=src_mysql_info,des_kwargs=des_mysql_info,binlog=self.binlog).start()
            if _binlog_file is None or _binlog_pos is None:
                sys.exit()
        '''============================================================================================================'''

        '''
        在源库利用replication协议建立主从链接
        如有全量导出使用导出开始时记录的binlog信息，不然使用传入参数的值
        '''
        self.__init_master_slave_conn()  # 初始化源库、目标库同步链接
        if self.daemon:
            _binlog_file, _binlog_pos, _excute_gtid = self.__get_daemon_info()
        Logging(msg='replication to master.............', level='info')


<<<<<<< HEAD
        if self.daemon or self.full_dump:
            rep_info = {'log_file': _binlog_file, 'log_pos': _binlog_pos, 'mysql_connection': self.conn,
                        'server_id': self.server_id, 'auto_position': self.auto_position, 'gtid': _excute_gtid}
            Logging(msg='binlog: {} position: {} gtid : {}'.format(_binlog_file, _binlog_pos, _excute_gtid),
                    level='info')
            '''初始化要记录得gtid'''
            _gtid = self.__gtid_set(_excute_gtid) if _excute_gtid else None

=======
        if self.full_dump:
            rep_info = {'log_file': _binlog_file, 'log_pos': _binlog_pos, 'mysql_connection': self.conn,
                        'server_id': self.server_id, 'auto_position': self.auto_position, 'gtid': _excute_gtid}
>>>>>>> 4ddcef19edcd96b31c1403fa3e2e920a9ecd5117
            ReplConn = ReplicationMysql(**rep_info).ReadPack()
        else:
            rep_info = {'log_file': self.binlog_file, 'log_pos': self.start_position, 'mysql_connection': self.conn,
                        'server_id': self.server_id, 'auto_position': self.auto_position, 'gtid': self.gtid}
<<<<<<< HEAD
            Logging(msg='binlog: {} position: {} gtid : {}'.format(self.binlog_file, self.start_position, self.gtid),
                    level='info')
            '''初始化要记录得gtid'''
            _gtid = self.__gtid_set(self.gtid) if self.gtid else None
=======
>>>>>>> 4ddcef19edcd96b31c1403fa3e2e920a9ecd5117
            ReplConn = ReplicationMysql(**rep_info).ReadPack()
        '''============================================================================================================'''

        table_struce_key = None
        next_pos = None
        binlog_file_name = _binlog_file if self.full_dump else self.binlog_file

<<<<<<< HEAD
        _mysql_conn = GetStruct(host=self.dhost, port=self.dport,user=self.duser,passwd=self.dpasswd)
        _mysql_conn.CreateTmp()


=======
        _mysql_conn = GetStruct(host=self.host, port=self.port,user=self.user,passwd=self.passwd,socket=self.unix_socket)
        _mysql_conn.CreateTmp()

        '''初始化要记录得gtid'''
        _gtid = self.__gtid_set(_excute_gtid) if _excute_gtid else None
>>>>>>> 4ddcef19edcd96b31c1403fa3e2e920a9ecd5117

        at_pos = _binlog_pos if self.full_dump else self.start_position
        if ReplConn:
            Logging(msg='replication succeed................', level='info')

            '''
            开始循环获取binlog
            仅对row_event、table_map_event、gtid_log_event、rotate_event、query_event
            row_event: 获取行数据
            table_map_event: 获取数据库名、表明、字段信息
            gtid_log_event、rotate_event、query_event：获取binlog基本信息记录与dump2db中
            '''
            while 1:
                try:
                    pkt = ReplConn._read_packet()

                    _parse_event = ParseEvent(packet=pkt,remote=True)
                    event_code, event_length ,next_pos= _parse_event.read_header()
                    if event_code is None or event_code in (binlog_events.UNKNOWN_EVENT,binlog_events.START_EVENT_V3):
                        continue

                    if event_code in (binlog_events.WRITE_ROWS_EVENT,binlog_events.UPDATE_ROWS_EVENT,binlog_events.DELETE_ROWS_EVENT):
                        '''
                        对过滤的thread_id，type进行判断
                        '''
                        if self.repl_mark:
                            pass
                        elif self.ithread:
                            if self.ithread == tmepdata.thread_id:
                                continue
                            if self.ignore_type and self.ignore[self.ignore_type] == event_code:
                                continue
                            self.__execute_code(_parse_event=_parse_event,event_code=event_code,
                                                    event_length=event_length,table_struce_key=table_struce_key)
                        else:
                            if self.ignore_type and self.ignore[self.ignore_type] == event_code:
                                continue
                            self.__execute_code(_parse_event=_parse_event, event_code=event_code,
                                                    event_length=event_length, table_struce_key=table_struce_key)

                    elif event_code == binlog_events.TABLE_MAP_EVENT:
                        '''
                        table_map_event获取后面row数据所再库、表基础信息
                        获取数据之后判断是否是需要同步的库、表，如果是将开启
                        xid事务，并设定一个xa事务是否开启的状态，因为一个
                        gtid事务可能包含多个table的操作，这样使一个gtid事
                        务都包含在一个事务中进行操作，并且在此对repl_mark
                        进行判定，如果是同步程序操作的事务再第一个table_map
                        将是对repl_mark进行操作，如代码中的实现再开始xa事务
                        第一个操作就是repl_mark
                        '''
                        tmepdata.database_name, tmepdata.table_name, tmepdata.cloums_type_id_list, tmepdata.metadata_dict=_parse_event.GetValue(type_code=event_code,event_length=event_length)  # 获取event数据
                        table_struce_key = '{}:{}'.format(tmepdata.database_name, tmepdata.table_name)
                        if tmepdata.database_name == 'repl_mark':
                            self.repl_mark = True
                        elif tmepdata.database_name in self.databases:
                            if self.tables:
                                if tmepdata.table_name in self.tables:
                                    if table_struce_key not in tmepdata.table_struct_list:
                                        column_list, pk_idex, column_type_list = _mysql_conn.GetColumn(
                                            tmepdata.database_name, tmepdata.table_name)
                                        tmepdata.table_struct_list[table_struce_key] = column_list
                                        tmepdata.table_pk_idex_list[table_struce_key] = pk_idex
                                        tmepdata.table_struct_type_list[table_struce_key] = column_type_list
                                    '''开启xa事务'''
                                    if self.xa is None:
                                        self.__xa_trasaction('start')
                                        self.__set_mark()
                                        self.xa = True
                            else:
                                if table_struce_key not in tmepdata.table_struct_list:
                                    column_list, pk_idex, column_type_list = _mysql_conn.GetColumn(tmepdata.database_name,
                                                                                                   tmepdata.table_name)
                                    tmepdata.table_struct_list[table_struce_key] = column_list
                                    tmepdata.table_pk_idex_list[table_struce_key] = pk_idex
                                    tmepdata.table_struct_type_list[table_struce_key] = column_type_list
                                '''开启xa事务'''
                                if self.xa is None:
                                    self.__xa_trasaction('start')
                                    self.__set_mark()
                                    self.xa = True

                    elif event_code == binlog_events.ROTATE_EVENT:
                            binlog_file_name = _parse_event.read_rotate_log_event(event_length=event_length)

                    elif event_code == binlog_events.QUERY_EVENT:
                        tmepdata.thread_id,_db,_statement = _parse_event.read_query_event(event_length=event_length)
                        _statement = _statement.decode('utf-8')
                        _db = _db.decode('utf-8')
                        if ('alter table' in _statement or 'ALTER TABLE' in _statement) and _db in self.databases:
                            self.__check_stat(self.__put_new_db(sql = 'use {}'.format(_db),args=[]))
                            self.__check_stat(self.__put_new_db(sql = _statement.replace('\n','').replace('\r',' '),args=[]))


                    elif event_code == binlog_events.GTID_LOG_EVENT:
<<<<<<< HEAD
                        '''
                        获取当前事务GTID，在此不做任何操作，因为不知道gtid包含的事务所属库、表
                        '''
                        __gtid,__gno_id = _parse_event.read_gtid_event(event_length=event_length)
                        _gtid[__gtid] = '1-{}'.format(__gno_id)

                    elif event_code == binlog_events.XID_EVENT:
                        '''
                        xid_event是整个gtid事务结束的标志
                        在此做所有事务的提交操作
                        '''
                        if self.repl_mark:
                            _apply_conn = None
                        else:
                            _apply_conn = self.destination_cur if self.xa else None
                            if tmepdata.database_name in self.databases:
                                if self.tables :
                                    if tmepdata.table_name in self.tables and self.xa:
                                        self.__xa_trasaction('prepare')
                                else:
                                    if self.xa:
                                        self.__xa_trasaction('prepare')

                        save_gtid_value = json.dumps(tmepdata.excute_gtid) if tmepdata.excute_gtid else None
                        if self.auto_position:
                            _mysql_conn.SaveStatus(logname=binlog_file_name, at_pos=at_pos, next_pos=next_pos,
                                                   server_id=self.server_id, gtid=save_gtid_value,
                                                   apply_conn=_apply_conn,xa=self.xa)

                        else:
                            _mysql_conn.SaveStatus(logname=binlog_file_name, at_pos=at_pos, next_pos=next_pos,
                                                       server_id=self.server_id,
                                                   gtid=save_gtid_value,
                                                   apply_conn=_apply_conn,xa=self.xa)

                        tmepdata.excute_gtid = _gtid.copy() if _gtid else  None

                        tmepdata.sql_list = []
                        self.xa = None
                        self.repl_mark = None
=======
                        __gtid,__gno_id = _parse_event.read_gtid_event(event_length=event_length)
                        _gtid[__gtid] = '1-{}'.format(__gno_id)
                    elif event_code == binlog_events.XID_EVENT:
                        if self.auto_position:
                            _mysql_conn.SaveStatus(logname=binlog_file_name, at_pos=at_pos, next_pos=next_pos,
                                                   server_id=self.server_id, gtid=json.dumps(_gtid))
                            tmepdata.excute_gtid = _gtid.copy()
                        else:
                            _mysql_conn.SaveStatus(logname=binlog_file_name, at_pos=at_pos, next_pos=next_pos,
                                                       server_id=self.server_id)
                        self.destination_conn.commit()
>>>>>>> 4ddcef19edcd96b31c1403fa3e2e920a9ecd5117
                        continue
                except pymysql.OperationalError:
                    '''链接断开重新注册'''
                    Logging(msg='retry to regist master', level='error')
                    ReplConn = self.__retry_regist_master(gtid=tmepdata.excute_gtid, binlog=binlog_file_name, position=at_pos)
                except:
                    Logging(msg=traceback.format_exc(),level='error')
                    ReplConn.close()
                    break

                at_pos = next_pos
        else:
            Logging(msg='replication failed................', level='error')
            self.destination_conn.close()
            self.conn.close()

<<<<<<< HEAD
    def __set_mark(self):
        '''
        所有目标库操作前都设置事务标签，用于双向同步的回环控制
        :return:
        '''
        sql = 'INSERT INTO repl_mark.mark_status(id,`status`) VALUES(%s,uuid_short()) ON DUPLICATE KEY UPDATE  `status`=uuid_short();'
        tmepdata.sql_list.append([sql,self.server_id])
        self.__check_stat(self.__put_new_db(sql, args=self.server_id))

    def __put_new_db(self,sql,args):
        '''
        追加binlog数据到目标库
        :param sql:
        :param args:
        :return:
        '''
=======
    def __put_new_db(self,sql,args):
>>>>>>> 4ddcef19edcd96b31c1403fa3e2e920a9ecd5117
        try:
            self.destination_cur.execute(sql,args)
        except pymysql.OperationalError:
            self.__retry_connection_destion()
            for row in tmepdata.sql_list:
                self.destination_cur.execute(row[0],row[1])
        except pymysql.InterfaceError:
            self.__retry_connection_destion()
            for row in tmepdata.sql_list:
                self.destination_cur.execute(row[0], row[1])
        except pymysql.Error:
            Logging(msg=[sql,args],level='error')
            Logging(msg=traceback.format_exc(),level='error')
            return None
        return True

    def __gtid_set(self,gtid):
        '''
        字典化gtid
        :param gtid:
        :return:
        '''
        _gtid_list = gtid.replace('\n','').split(',')
        _gtid_dict = {}
        for uuid in _gtid_list:
            _u = uuid.split(':')
            _gtid_dict[_u[0]] = _u[1]

        return _gtid_dict


    def __retry_regist_master(self,gtid=None,binlog=None,position=None):
<<<<<<< HEAD
        '''
        尝试重新注册主从链接
        :param gtid:
        :param binlog:
        :param position:
        :return:
        '''
=======
>>>>>>> 4ddcef19edcd96b31c1403fa3e2e920a9ecd5117
        import time
        if gtid:
            gtid = ','.join(['{}:{}'.format(uuid,gtid[uuid]) for uuid in gtid])
        while True:
            try:
                self.conn = InitMyDB(mysql_host=self.host, mysql_port=self.port, mysql_user=self.user,
<<<<<<< HEAD
                                     mysql_password=self.passwd, unix_socket=self.unix_socket,ssl=self.ssl_auth).Init()
=======
                                     mysql_password=self.passwd, unix_socket=self.unix_socket).Init()
>>>>>>> 4ddcef19edcd96b31c1403fa3e2e920a9ecd5117
                rep_info = {'log_file': binlog, 'log_pos': position, 'mysql_connection': self.conn,
                            'server_id': self.server_id, 'auto_position': self.auto_position, 'gtid': gtid}
                ReplConn = ReplicationMysql(**rep_info).ReadPack()
                Logging(msg='regist master ok !', level='info')
                return ReplConn
            except pymysql.Error:
                Logging(msg=traceback.format_list(),level='error')

<<<<<<< HEAD
            time.sleep(1)

    def __retry_connection_destion(self):
        '''
        目标库链接丢失重试5次，如果5次都失败将退出整个程序
        :return:
        '''
        import time
        for i in range(5):
            try:
                self.destination_conn = InitMyDB(mysql_host=self.dhost, mysql_port=self.dport, mysql_user=self.duser,
                                                 mysql_password=self.dpasswd,auto_commit=True).Init()
                self.destination_cur = self.destination_conn.cursor()
                if self.binlog is None:
                    self.destination_cur.execute('set sql_log_bin=0;')  # 设置binlog参数
                self.destination_cur.execute('SET SESSION wait_timeout = 2147483;')
                self.__xa_trasaction('rollback')
                return True
            except:
                Logging(msg=traceback.format_list(),level='error')
            time.sleep(1)
        else:
            Logging(msg='try 5 times to fail for conncetion destination db,exist now',level='error')
            sys.exit()

    def __check_stat(self,state):
        if state:
            pass
        else:
            self.__xa_trasaction('rollback')
            Logging(msg='failed!!!!', level='error')
            sys.exit()

    def __xa_trasaction(self,state=None):
        if state == 'start':
            self.__check_xaid()
            self.destination_cur.execute('XA START "apply"')
        elif state == 'prepare':
            self.destination_cur.execute('XA END "apply"')
            self.destination_cur.execute('XA PREPARE "apply"')
        elif state == 'rollback':
            self.__check_xaid()

    def __check_xaid(self):
        '''
        检查是否存在同名的xa事务，如果存在直接回滚释放
        :return:
        '''
        self.destination_cur.execute('xa recover')
        result = self.destination_cur.fetchall()
        if result:
            for xa in result:
                if xa['data'] == 'apply':
                    self.destination_cur.execute('XA ROLLBACK "apply"')

    def __get_daemon_info(self):
        '''
        重启获取已经读取的binlog信息
        :return:
        '''
        self.destination_cur.execute('select logname,next_pos,excute_gtid from dump2db.dump_status where  id = %s;',self.server_id)
        result = self.destination_cur.fetchall()
        _gtid = []
        print(result)
        if result[0]['excute_gtid']:
            print(result[0]['excute_gtid'])
            gtid = eval(result[0]['excute_gtid'])
            _gtid = ['{}:{}'.format(uuid,gtid[uuid]) for uuid in gtid]
        return result[0]['logname'],result[0]['next_pos'],','.join(_gtid)
=======
            time.sleep(1)
>>>>>>> 4ddcef19edcd96b31c1403fa3e2e920a9ecd5117
