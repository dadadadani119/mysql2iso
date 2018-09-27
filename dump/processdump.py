# -*- encoding: utf-8 -*-
'''
@author: Great God
'''
import queue
import traceback
import threading
import sys
from .prepare import Prepare
from .dump import Dump
sys.path.append("..")
from lib.Loging import Logging
from lib.InitDB import InitMyDB
from mode.phoenix.InitDB import InitDB as InitPhoenixDB

class ThreadDump(threading.Thread):
    def __init__(self, queue, dump_pro,chunk_list,database,table,idx,pri_idx,tbl,cols,bytes_col_list=None):
        threading.Thread.__init__(self)
        self.queue = queue
        self.dump_pro = dump_pro
        self.chunk_list = chunk_list
        self.database = database
        self.table = table
        self.idx = idx
        self.pri_idx = pri_idx
        self.tbl = tbl
        self.cols = cols
        self.bytes_col_list = bytes_col_list
    def run(self):
        try:
            __tuple_ = [self.database,self.table,self.idx,self.pri_idx,self.chunk_list,
                        self.bytes_col_list,self.tbl,self.cols]
            self.dump_pro.dump_to_new_db(*__tuple_)
            self.queue.put('1001')
        except:
            Logging(msg=traceback.format_exc(),level='error')
            return

class processdump(Prepare):
    def __init__(self,threads=None,dbs=None,tables=None,src_kwargs=None,des_kwargs=None,binlog=None,
                 map_conf=None,queal_struct=None,destination_type=None,jar=None,jar_conf=None):
        super(processdump,self).__init__(threads=threads,src_kwargs=src_kwargs,des_kwargs=des_kwargs,
                                         jar=jar,jar_conf=jar_conf,destination_type=destination_type)

        '''异构对应项'''
        self.map_conf = map_conf
        self.queal_struct = queal_struct
        self._tmp_queal_struct = None
        self.jar,self.jar_conf = jar,jar_conf
        self.destination_type = destination_type
        ''''''

        self.binlog = binlog
        self.des_kwargs = des_kwargs

        self.databases = dbs
        self.tables = tables
        self.queue = queue.Queue()

        self.des_mysql_conn = None
        self.des_mysql_cur = None
        self.conn,self.cur = None,None
        self.dump = None

        self.table_column_struct = []

        self.__init_info()

    def __init_info(self,des=None):
        '''
        初始化数据库主链接信息
        :return:
        '''
        if des:
            if self.destination_type == 'phoenix':
                self.des_mysql_conn = InitPhoenixDB(user=self.des_conn_info['mysql_user'],
                                                    passwd=self.des_conn_info['mysql_password'],
                                                    host=self.des_conn_info['mysql_host'],
                                                    port=self.des_conn_info['mysql_port'],
                                                    jar=self.jar,jar_conf=self.jar_conf).Init()
                self.des_mysql_cur = self.des_mysql_conn.cursor()
            else:
                self.des_mysql_conn = InitMyDB(**self.des_kwargs).Init()
                self.des_mysql_cur = self.des_mysql_conn.cursor()
                if self.binlog is None:
                    self.des_mysql_cur.execute('set sql_log_bin=0')
                self.des_mysql_cur.execute('SET SESSION wait_timeout = 2147483;')
            self.des_thread_list.append({'conn': self.des_mysql_conn, 'cur': self.des_mysql_cur})
            self.dump = Dump(cur=self.cur, des_conn=self.des_mysql_conn, des_cur=self.des_mysql_cur,
                             destination_type=self.destination_type,table_column_struct=self.table_column_struct,
                             des_conn_info=self.des_conn_info,jar=self.jar,jar_conf=self.jar_conf,binlog=self.binlog)
        else:
            self.conn, self.cur = self.init_conn(primary_t=True)


    def __get_queal_conf(self):
        '''
        获取异构配置
        :return:
        '''
        from lib.get_conf import GetStruct

        self._tmp_queal_struct = GetStruct(self.map_conf).get()

    def __equeal(self,database,tables,threads=None):
        for table in tables:
            state = self.__queal_table_check(database, table)
            if state:
                tbl, cols = self.__queal_col_check(database, table)
                _parmeter = [database, table, tbl, cols]
                if threads:
                    self.__mul_dump_go(*_parmeter)
                else:
                    self.__dump_go(database=database,tablename=table,tbl=tbl,cols=cols)

    def __queal_start(self,threads=None,database=None):
        if database in self._tmp_queal_struct:
            if self.tables:
                self.__equeal(database,self.tables,threads=threads)
            else:
                tables = self.__queal_table_check(database,check=True)
                if tables:
                    self.__equeal(database,tables,threads=threads)

    def __queal_table_check(self,database,tablename=None,check=None):
        '''
        检查异构表是否存在
        :param database:
        :param tablename:
        :return:
        '''
        db_struct = self._tmp_queal_struct[database]
        if check:
            tables = []
            for table in db_struct:
                tables.append(table)
            return tables

        if tablename in db_struct:
            return True
        return None

    def __queal_col_check(self,database,tablename):
        '''
        返回异构对应库、表、字段关系
        :param database:
        :param tablename:
        :return:
        '''
        db_struct = self._tmp_queal_struct[database]
        tbl_info = db_struct[tablename]
        tbl = tbl_info['tbl']
        cols = None
        if 'cols' in tbl_info:
            cols = tbl_info['cols']
        return tbl,cols

    def start(self):
        '''
        所有在线导出操作将在该函数内部直接完成，直至退出并返回binlog相关信息
        binlog信息在所有链接初始化完成后获取，因为所有链接都采用的SNAPSHOT
        因此主链接会执行全局读锁，但非常短暂，在所有链接初始化完成将释放
        :return:
        '''
        if self.queal_struct:
            self.__get_queal_conf()

        binlog_file,binlog_pos,excute_gtid = self.master_info(cur=self.cur)
        if (binlog_file and binlog_pos) or (excute_gtid):
            pass
        else:
            self.cur.execute('UNLOCK TABLES')
            self.close(self.cur,self.conn)
            Logging(msg='invalid master info , file {} position {} gtid {}'.format(binlog_file,binlog_pos,
                                                                                   excute_gtid),level='error')
            sys.exit()

        '''初始化源库所有链接'''
        if self.threads and self.threads > 1:
            self.init_conn()
            # self.init_des_conn(binlog=self.binlog)

        self.cur.execute('UNLOCK TABLES')

        if self.threads and self.threads > 1:
            '''多线程导出'''
            for database in self.databases:
                if self.queal_struct:
                    self.__queal_start(threads=True,database=database)
                elif self.tables:
                    for tablename in self.tables:
                        _parmeter = [database,tablename]
                        self.__mul_dump_go(*_parmeter)

                else:
                    tables = self.get_tables(cur=self.cur, db=database)
                    for tablename in tables:
                        _parmeter = [database, tablename]
                        self.__mul_dump_go(*_parmeter)

        else:
            '''单线程导出'''
            for database in self.databases:
                if self.queal_struct:
                    self.__queal_start(database=database)
                elif self.tables:
                    for tablename in self.tables:
                        _parameter = [database,tablename]
                        self.__dump_go(*_parameter)
                else:
                    '''全库导出'''
                    tables = self.get_tables(cur=self.cur,db=database)
                    for table in tables:
                        _parameter = [database, table]
                        self.__dump_go(*_parameter)

        '''操作完成关闭所有数据库链接'''
        if self.threads and self.threads > 1:
            for thread in self.thread_list:
                self.close(thread['cur'],thread['conn'])
            for thread in self.des_thread_list:
                self.close(thread['cur'], thread['conn'])
        return binlog_file,binlog_pos,excute_gtid

    def __dump_go(self,database,tablename,idx_name=None,pri_idx=None,max_min=None,bytes_col_list=None,tbl=None,cols=None):
        '''
        单线程导出函数
        :param database:
        :param tablename:
        :return:
        '''
        self.__getcolumn(database, tablename)
        if len(self.des_thread_list) < 1:
            self.__init_info(des=True)
        stat = self.queal_struct if self.queal_struct else self.dump.prepare_structe(database=database, tablename=tablename)
        if stat:
            if idx_name is None and pri_idx is None:
                idx_name,pri_idx = self.check_pri(cur=self.cur, db=database, table=tablename)
                bytes_col_list = self.check_byte_col(cur=self.cur, db=database, table=tablename)
                max_min = self.split_data(
                    self.cur,self.get_max_min(cur=self.cur,databases=database,tables=tablename,index_name=idx_name),
                    idx_name,database,tablename)
            if max_min and max_min[0]:
                dump = Dump(cur=self.cur, des_conn=self.des_mysql_conn, des_cur=self.des_mysql_cur,
                            destination_type=self.destination_type, table_column_struct=self.table_column_struct,
                            des_conn_info=self.des_conn_info, jar=self.jar, jar_conf=self.jar_conf, binlog=self.binlog)
                dump.dump_to_new_db(database=database, tablename=tablename, idx=idx_name, pri_idx=pri_idx,
                                     chunk_list=max_min,bytes_col_list=bytes_col_list,tbl=tbl,cols=cols)
        else:
            Logging(msg='Initialization structure error', level='error')
            sys.exit()

    def __mul_dump_go(self,database,tablename,tbl=None,cols=None):
        '''
        多线程导出函数
        尽量选择合适的索引，通过索引值拆分每个线程数操作的值区间
        :param database:
        :param tablename:
        :return:
        '''
        idx_name, pri_idx = self.check_pri(cur=self.cur, db=database, table=tablename)

        chunks_list,uli = self.get_chunks(cur=self.cur, databases=database, tables=tablename,index_name=idx_name)
        #bytes_col_list = self.check_byte_col(cur=self.cur,db=database,table=tablename)
        if chunks_list is None:
            Logging(msg='this table {} chunks_list is None,maybe this table not data'.format(tablename),level='warning')
            return
        if uli:
            '''多线程'''

            '''初始化目标库所有并发链接及函数'''
            if len(self.des_thread_list) < 1:
                self.__init_info(des=True)
                if self.threads and self.threads > 1:
                    # self.init_conn()
                    self.init_des_conn(binlog=self.binlog)
            stat = self.queal_struct if self.queal_struct else self.dump.prepare_structe(database=database,
                                                                                         tablename=tablename)
            if stat:
                self.__getcolumn(database,tablename)
                for t in range(len(self.thread_list)):
                    dump = Dump(cur=self.thread_list[t]['cur'], des_conn=self.des_thread_list[t]['conn'],
                                des_cur=self.des_thread_list[t]['cur'],destination_type=self.destination_type,
                                table_column_struct=self.table_column_struct,des_conn_info=self.des_conn_info,
                                jar=self.jar,jar_conf=self.jar_conf,binlog=self.binlog)
                    __dict_ = [self.queue, dump, chunks_list[t], database, tablename, idx_name, pri_idx,tbl,cols]
                    _t = ThreadDump(*__dict_)
                    _t.start()
                self.__get_queue()
            else:
                Logging(msg='Initialization structure error', level='error')
                sys.exit()

        else:
            '''单线程'''
            self.__dump_go(database,tablename,idx_name,pri_idx,chunks_list,tbl=tbl,cols=cols)


    def __get_queue(self):
        '''
        获取queue中的数据个数，直到取回个数与并发线程数相等才退出
        :return:
        '''
        _count = len(self.thread_list)
        _tmp_count = 0
        while _tmp_count < _count:
            value = self.queue.get()
            if value:
                _tmp_count += 1

    def __getcolumn(self,*args):
        '''args顺序 database、tablename'''
        self.table_column_struct = []
        sql = 'select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_schema=%s and table_name=%s order by ORDINAL_POSITION;'
        self.cur.execute(sql,args=args)

        result = self.cur.fetchall()
        for idex,row in enumerate(result):
            self.table_column_struct.append(row['COLUMN_NAME'])

