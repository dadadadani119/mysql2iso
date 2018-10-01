# -*- encoding: utf-8 -*-
'''
@author: Great God
'''
import sys,pymysql,traceback
import json,time
from .ErrorCode import ErrorCode
from .Loging import Logging
from .InitDB import InitMyDB
from .escape import escape
sys.path.append("..")
from dump.processdump import processdump
from binlog.Replication import ReplicationMysql
from binlog.ParseEvent import ParseEvent
from binlog.PrepareStructure import GetStruct
from binlog.Metadata import binlog_events

class tmepdata:
    '''binlog解析及拼装sql所需的元数据'''
    database_name,table_name,cloums_type_id_list,metadata_dict = None,None,None,None

    #下面三个记录类型都以'db:tab' 做key,对应数据做value
    table_struct_list = {}              #字段名列表
    table_pk_idex_list = {}             #主键索引列表
    table_struct_type_list = {}         #字段类型列表

    unsigned_list = {}                  #无符号整型字段列表
    thread_id = None
    excute_gtid = {}
    sql_list = []                       #记录一个GTID事务的所有sql语句


class OperationDB(escape):
    def __init__(self,**kwargs):
        super(OperationDB,self).__init__()
        self.destnation_type = kwargs['destnation_type']
        self.jar = kwargs['jar']
        self.jar_conf = kwargs['jar_conf']

        self.map_conf = kwargs['map_conf']
        self.queal_struct = kwargs['queal_struct']
        self.ssl_auth = {'cert':kwargs['cert'],'key':kwargs['key']} if kwargs['ssl'] else None
        self.daemon = kwargs['daemon']
        self.queue = kwargs['queue']
        self.lookback = kwargs['lookback']
        #print('op: {}'.format(self.queue))
        self.full_dump = kwargs['full_dump']                                                                                                        #是否全量导出
        self.threads = kwargs['threads']                                                                                                            #全量导出时并发线程

        self.host,self.port,self.user,self.passwd = kwargs['host'],kwargs['port'],kwargs['user'],kwargs['passwd']                                   #源库连接相关信息
        self.unix_socket = kwargs['socket']

        self.dhost,self.dport,self.duser,self.dpasswd = kwargs['dhost'],kwargs['dport'],kwargs['duser'],kwargs['dpasswd']                           #目标库连接相关信息
        self.binlog = kwargs['binlog']                                                                                                              #是否在目标库记录binlog的参数

        self.destination_conn = None
        self.destination_cur = None
        self.conn = None
        self.cur = None

        self.repl_mark = None                                                                                                                       #读取binlog记录该GTID事务是否拥有标签操作
        self.repl_mark_status = None                                                                                                                #插入数据时记录该事务是否已做标签操作
        self.apply_conn = None
        self.crc = None
        self._status_conn = None
        self.gno_uid = None                                                                                                                         #binlog中读取到当前事务的gtid
        self.gno_id = None                                                                                                                          #binlog读取到gtid的gno

        self.at_pos = None

        self.trancaction_list = []                                                                                                                  #已执行的事务sql,用于重连之后重新执行

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

        self.shost = kwargs['shost']
        self.sport = kwargs['sport']
        self.suser = kwargs['suser']
        self.spassword = kwargs['spassword']
        self.sbinlog = kwargs['sbinlog']


    def __init_master_slave_conn(self):
        '''
        初始化同步所需的源库、目标库的链接
        :return:
        '''
        for i in range(60):
            self.conn = InitMyDB(mysql_host=self.host, mysql_port=self.port, mysql_user=self.user,
                                 mysql_password=self.passwd, unix_socket=self.unix_socket, ssl=self.ssl_auth).Init()

            try:
                self.cur = self.conn.cursor()
                self.cur.execute('SET SESSION wait_timeout = 2147483;')
                return
            except pymysql.Error as e:
                Logging(msg=e.args,level='error')
            time.sleep(1)
        else:
            Logging(msg='source db connection failed !!!',level='error')
            sys.exit()

    def __init_status_conn(self):

        self._status_conn = GetStruct(host=self.shost, port=self.sport, user=self.suser, passwd=self.spassword,
                                      binlog=self.sbinlog)  # 状态库链接类初始化
        self._status_conn.CreateTmp()

    def __execute_code(self,_parse_event,event_code,event_length,table_struce_key):
        '''
        在此需要对库、表进行再次判断，因为在一个GTID中可能有包含非需要的库表操作，这样会时mark标签失效
        :param _parse_event:
        :param event_code:
        :param event_length:
        :param table_struce_key:
        :return:
        '''
        if tmepdata.database_name in self.databases:
            if self.tables:
                if tmepdata.table_name in self.tables:
                    _values = _parse_event.GetValue(type_code=event_code, event_length=event_length,
                                        cloums_type_id_list=tmepdata.cloums_type_id_list,
                                        metadata_dict=tmepdata.metadata_dict,
                                        unsigned_list=self.__check_struce(table_struce_key))
                    tmepdata.sql_list.append([_values,tmepdata.database_name,tmepdata.table_name,self.at_pos,event_code])
            else:
                _values = _parse_event.GetValue(type_code=event_code, event_length=event_length,
                                                cloums_type_id_list=tmepdata.cloums_type_id_list,
                                                metadata_dict=tmepdata.metadata_dict,
                                                unsigned_list=self.__check_struce(table_struce_key))
                tmepdata.sql_list.append([_values, tmepdata.database_name, tmepdata.table_name, self.at_pos,event_code])

    def __check_struce(self,table_struce_key):
        if table_struce_key in tmepdata.table_struct_type_list:
            return tmepdata.table_struct_type_list[table_struce_key]
        else:
            Logging(msg='this struce name {} not in table_struct_type_list'.format(table_struce_key),level='error')
            sys.exit()

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
                              'mysql_password':self.passwd,'unix_socket':self.unix_socket,'ssl':self.ssl_auth}
            _binlog_file,_binlog_pos,_excute_gtid = processdump(threads=self.threads,dbs=self.databases,tables=self.tables,
                                                   src_kwargs=src_mysql_info,des_kwargs=des_mysql_info,binlog=self.binlog,
                                                                map_conf=self.map_conf,queal_struct=self.queal_struct,
                                                                destination_type=self.destnation_type,jar=self.jar,
                                                                jar_conf=self.jar_conf).start()
            if _binlog_file is None or _binlog_pos is None:
                sys.exit()
        '''============================================================================================================'''

        '''
        在源库利用replication协议建立主从链接
        如有全量导出使用导出开始时记录的binlog信息，不然使用传入参数的值
        '''
        self.__init_master_slave_conn()  # 初始化源库、目标库同步链接
        self.__get_all_table_struct()
        # self.cur.close()
        self.queue.put({'table_struct': [tmepdata.table_struct_list, tmepdata.table_pk_idex_list]})

        if self.daemon:
            self.__init_status_conn()  # 初始化状态库
            _binlog_file, _binlog_pos, _excute_gtid ,gtid_uid = self._status_conn.get_daemon_info(self.server_id)
            self._status_conn.close()
            _excute_gtid = ','.join(['{}:{}'.format(uuid, _excute_gtid[uuid]) for uuid in _excute_gtid])
        Logging(msg='replication to master.............', level='info')


        if self.daemon or self.full_dump:
            self.cur.close()
            rep_info = {'log_file': _binlog_file, 'log_pos': _binlog_pos, 'mysql_connection': self.conn,
                        'server_id': self.server_id, 'auto_position': self.auto_position, 'gtid': _excute_gtid}
            Logging(msg='binlog: {} position: {} gtid : {}'.format(_binlog_file, _binlog_pos, _excute_gtid),
                    level='info')
            '''初始化要记录得gtid'''
            _gtid = self.__gtid_set(_excute_gtid) if _excute_gtid else None

            ReplConn = ReplicationMysql(**rep_info).ReadPack()
        else:
            self.__check_repl_info()
            self.cur.close()
            rep_info = {'log_file': self.binlog_file, 'log_pos': self.start_position, 'mysql_connection': self.conn,
                        'server_id': self.server_id, 'auto_position': self.auto_position, 'gtid': self.gtid}
            Logging(msg='binlog: {} position: {} gtid : {}'.format(self.binlog_file, self.start_position, self.gtid),
                    level='info')
            '''初始化要记录得gtid'''
            _gtid = self.__gtid_set(self.gtid) if self.gtid else None
            ReplConn = ReplicationMysql(**rep_info).ReadPack()
        '''============================================================================================================'''

        table_struce_key = None
        binlog_file_name = _binlog_file if self.full_dump else self.binlog_file




        next_pos = _binlog_pos if self.full_dump else self.start_position #开始读取的binlog位置

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
                self.at_pos = next_pos
                try:
                    pkt = ReplConn._read_packet()
                except pymysql.OperationalError:
                    '''链接断开重新注册'''
                    Logging(msg='retry to regist master', level='error')
                    ReplConn = self.__retry_regist_master(gtid=tmepdata.excute_gtid, binlog=binlog_file_name,
                                                          position=self.at_pos)
                except:
                    Logging(msg=traceback.format_exc(), level='error')
                    ReplConn.close()

                _parse_event = ParseEvent(packet=pkt,remote=True)
                event_code, event_length ,next_pos= _parse_event.read_header()
                if event_code is None or event_code in (binlog_events.UNKNOWN_EVENT,binlog_events.START_EVENT_V3):
                    continue

                if event_code in (binlog_events.WRITE_ROWS_EVENT,binlog_events.UPDATE_ROWS_EVENT,binlog_events.DELETE_ROWS_EVENT):
                    '''
                    对过滤的thread_id，type进行判断
                    '''
                    if self.lookback:
                        if self.repl_mark:
                            continue
                    if self.repl_mark_status:
                        if self.ithread:
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
                    table_map获取表结构等信息，在此实现的功能有：
                    1、判断是否是需要同步的库、表
                    2、如果满足同步要求，在table_map后将是row数据信息，因此在这对repl_mark标签进行操作，又由于一个GTID可能包含多个表
                    操作，故在此对标签库操作增加状态，一个GTID事务只做一次标签库操作
                    3、接受数据判断是否是repl_mark的操作，如果是将设置状态跳过当前GTID所有操作，用于回环控制
                    4、有可能未使用gtid模式，所以在table_map进行标签操作，在xid_even进行结束操作
                    '''
                    tmepdata.database_name, tmepdata.table_name, tmepdata.cloums_type_id_list, tmepdata.metadata_dict=_parse_event.GetValue(type_code=event_code,event_length=event_length)  # 获取event数据
                    table_struce_key = '{}:{}'.format(tmepdata.database_name, tmepdata.table_name)
                    if self.repl_mark:
                        continue
                    elif tmepdata.database_name == 'repl_mark':
                        self.repl_mark = True
                    elif tmepdata.database_name in self.databases:
                        if self.tables:
                            if tmepdata.table_name in self.tables:
                                '''写入标签'''
                                if self.repl_mark_status is None:
                                    #self.__set_mark()
                                    self.repl_mark_status = True
                        else:
                            '''写入标签'''
                            if self.repl_mark_status is None:
                                #self.__set_mark()
                                self.repl_mark_status = True

                elif event_code == binlog_events.ROTATE_EVENT:
                        binlog_file_name = _parse_event.read_rotate_log_event(event_length=event_length)

                elif event_code == binlog_events.QUERY_EVENT:
                    if self.repl_mark:
                        continue
                    tmepdata.thread_id,_db,_statement = _parse_event.read_query_event(event_length=event_length)

                elif event_code == binlog_events.GTID_LOG_EVENT:
                    '''
                    获取当前事务GTID，在此不做任何操作，因为不知道gtid包含的事务所属库、表
                    '''
                    self.gno_uid,self.gno_id = _parse_event.read_gtid_event(event_length=event_length)
                    _gtid[self.gno_uid] = '1-{}'.format(self.gno_id)

                elif event_code == binlog_events.XID_EVENT:
                    '''
                    xid_event是整个gtid事务结束的标志
                    在此做所有事务的提交操作
                    '''
                    save_gtid_value = json.dumps(_gtid) if _gtid else None

                    tracaction_value = {'gtid':save_gtid_value,'gno_uid':self.gno_uid,'gno_id':self.gno_id,
                                        'binlog':binlog_file_name,'at_pos':self.at_pos,'next_pos':next_pos,
                                        'value_list':tmepdata.sql_list}
                    append_state = self.append_data(values=tracaction_value)
                    if append_state:
                        tmepdata.excute_gtid = _gtid.copy() if _gtid else  None

                        tmepdata.sql_list = []
                        self.xa = None
                        self.repl_mark_status = None
                        self.repl_mark = None
                        continue
                    else:
                        Logging(msg='the binlog queue is full !!!!!',level='error')
                        sys.exit()

        else:
            Logging(msg='replication failed................', level='error')

            self.conn.close()


    def __set_mark(self):
        '''
        所有目标库操作前都设置事务标签，用于双向同步的回环控制
        :return:
        '''
        sql = 'INSERT INTO repl_mark.mark_status(id,gtid,gno_id) VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE  gtid=%s,gno_id=%s;'
        tmepdata.sql_list.append([sql,[self.server_id,self.gno_uid,self.gno_id,self.gno_uid,self.gno_id]])


    def __get_all_table_struct(self):
        '''
        获取所有需要同步的表结构
        :return:
        '''
        sql = 'select TABLE_SCHEMA,TABLE_NAME from INFORMATION_SCHEMA.COLUMNS GROUP BY TABLE_SCHEMA,TABLE_NAME;'
        self.__check_stat(self.__raise_sql(sql=sql))

        result = self.cur.fetchall()
        if result:
            for row in result:
                table_schema = row['TABLE_SCHEMA']
                table_name = row['TABLE_NAME']
                table_struce_key = '{}:{}'.format(table_schema, table_name)
                if table_schema in self.databases:
                    if self.tables:
                        if table_name in self.tables:
                            tmepdata.table_struct_list[table_struce_key],tmepdata.table_pk_idex_list[table_struce_key],\
                            tmepdata.table_struct_type_list[table_struce_key] = self.__getcolumn(table_schema,table_name)
                    else:
                        tmepdata.table_struct_list[table_struce_key], tmepdata.table_pk_idex_list[table_struce_key],\
                        tmepdata.table_struct_type_list[table_struce_key] = self.__getcolumn(table_schema, table_name)


    def __getcolumn(self,*args):
        '''args顺序 database、tablename'''
        column_list = []
        column_type_list = []

        sql = 'select COLUMN_NAME,COLUMN_KEY,COLUMN_TYPE from INFORMATION_SCHEMA.COLUMNS where table_schema=%s and table_name=%s order by ORDINAL_POSITION;'
        self.__check_stat(self.__raise_sql(sql = sql,args=args))

        result = self.cur.fetchall()
        pk_idex = []
        for idex,row in enumerate(result):
            column_list.append(row['COLUMN_NAME'])
            column_type_list.append(row['COLUMN_TYPE'])
            if row['COLUMN_KEY'] == 'PRI':
                pk_idex.append(idex)
        return column_list,pk_idex,column_type_list

    def __check_repl_info(self):
        '''
        在没有提供任何同步所需的binlog参数情况下
        获取当前位置进行同步
        :return:
        '''
        if all([self.binlog_file,self.start_position,self.gtid]):
            pass
        else:
            sql = 'show master status'
            self.__check_stat(self.__raise_sql(sql=sql))
            result = self.cur.fetchall()
            _re = result[0]
            self.binlog_file,self.start_position,self.gtid= _re['File'],_re['Position'],str(_re['Executed_Gtid_Set']).replace('\n','')


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
        '''
        尝试重新注册主从链接
        :param gtid:
        :param binlog:
        :param position:
        :return:
        '''
        import time
        if gtid:
            gtid = ','.join(['{}:{}'.format(uuid,gtid[uuid]) for uuid in gtid])
        while True:
            try:
                self.conn = InitMyDB(mysql_host=self.host, mysql_port=self.port, mysql_user=self.user,
                                     mysql_password=self.passwd, unix_socket=self.unix_socket,ssl=self.ssl_auth).Init()
                if self.conn:
                    rep_info = {'log_file': binlog, 'log_pos': position, 'mysql_connection': self.conn,
                                'server_id': self.server_id, 'auto_position': self.auto_position, 'gtid': gtid}
                    try:
                        ReplConn = ReplicationMysql(**rep_info).ReadPack()
                    except pymysql.Error as e:
                        Logging(msg=traceback.format_exc(),level='error')
                        if ErrorCode[e.args[0]]:
                            self.__retry_regist_master(gtid=gtid,binlog=binlog,position=position)
                    Logging(msg='regist master ok !', level='info')
                    return ReplConn
            except pymysql.Error:
                Logging(msg=traceback.format_list(),level='error')

            time.sleep(1)

    def __raise_sql(self, sql, args=[]):
        '''
        追加binlog数据到目标库
        :param sql:
        :param args:
        :return:
        '''
        args = self.escape_string(args) if args else []
        try:
            self.cur.execute(sql,args)
        except pymysql.Error as e:
            Logging(msg='{}'.format(e.args), level='error')
            if e.args[0] in ErrorCode:
                if ErrorCode[e.args[0]]:
                    self.__retry_execute(sql=sql, args=args)
                    return True
            Logging(msg='sql:{},values:{}'.format(sql, args), level='error')
            Logging(msg=e, level='error')
            return None
        except:
            Logging(msg=traceback.format_exc(), level='error')
            return None
        return True

    def __retry_execute(self, sql, args=[]):
        '''
        异常重试
        :param sql: sql语句
        :param args: 参数列表
        :param type: 是否需要重新执行该sql
        :param retry: 是否是重新执行的sql
        :return:
        '''
        self.__retry_connection_destion()
        self.__check_stat(self.__raise_sql(sql, args))


    def __retry_connection_destion(self):
        '''
        目标库链接丢失重试60次，如果60次都失败将退出整个程序
        使用30次的原因是有可能目标数据在发生宕机切换，如果30
        秒都无法完成重连那表示数据库已经宕机或无法链接
        :return:
        '''
        import time
        for i in range(60):
            Logging(msg='connection to source db try agian!!!', level='info')
            try:
                self.conn = InitMyDB(mysql_host=self.host, mysql_port=self.port,
                                                 mysql_user=self.user,
                                                 mysql_password=self.password,
                                                 unix_socket=self.unix_socket,
                                                 ssl=self.ssl_auth).Init()
                if self.conn:
                    self.cur = self.conn.cursor()
                    Logging(msg='connection success!!!', level='info')
                    try:
                        self.cur.execute('SET SESSION wait_timeout = 2147483;')
                        return True
                    except pymysql.Error as e:
                        Logging(msg=e.args,level='error')
                        if ErrorCode[e.args[0]]:
                            self.__retry_connection_destion()
            except:
                Logging(msg=traceback.format_exc(), level='error')
            time.sleep(1)
        else:
            Logging(msg='try 60 times to fail for conncetion source db,exist now', level='error')
            sys.exit()

    def __check_stat(self, state):
        if state:
            pass
        else:
            Logging(msg='failed!!!!', level='error')
            sys.exit()


    def append_data(self,values):
        '''
        向队列写入gtid事务数据,超时时间60秒,60秒内队列为被消费将直接退出
        :param values: {'gtid':{},'gno_uid':'',gno_id:123,'binlog':'','at_pos':123,'next_pos':123,'sql_list':[[sql,[args...],db,tabl],....]}
        :return:
        '''
        for i in range(60):
            if self.queue.full():
                time.sleep(1)
                continue
            self.queue.put(values)
            return True
        else:
            return False


