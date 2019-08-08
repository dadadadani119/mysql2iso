# -*- encoding: utf-8 -*-
'''
@author: xiao cai niao
'''
import pymysql
import sys,time
import traceback
sys.path.append("..")
from lib.Loging import Logging
from lib.escape import escape
from lib.InitDB import InitMyDB
from lib.ErrorCode import ErrorCode
from lib.valuseiso import Iso


class Dump(escape):
    def __init__(self,**kwargs):
        super(Dump,self).__init__()
        self.mysql_cur = kwargs['cur']

        self.des_conn_info = kwargs['des_conn_info']
        self.des_mysql_conn = kwargs['des_conn'] if 'des_conn' in kwargs else None
        self.des_mysql_cur = kwargs['des_cur'] if 'des_cur' in kwargs else None
        self.destination_type = kwargs['destination_type']
        self.table_column_struct = kwargs['table_column_struct']
        self.result = None
        self.jar = kwargs['jar'] if 'jar' in kwargs else None
        self.jar_conf = kwargs['jar_conf'] if 'jar_conf' in kwargs else None
        self.binlog = kwargs['binlog']
        self.database = None
        self.sql = None
        self.queal_struct = None
        if self.destination_type == 'postgresql':
            import psycopg2
        elif self.destination_type == 'phoenix':
            import phoenixdb
            from mode.phoenix.InitDB import InitDB as InitPhoenixDB

    def prepare_structe(self,database,tablename):
        '''
        在目标库准备对于的数据库、表结构
        目标库的数据表如果存在将直接删除
        如果目标表有数据需要注意是否可以直接删除
        :param database:
        :param tablename:
        :return:
        '''
        if self.destination_type != 'phoenix':
            self.__check_stat(self.__raise_sql(sql='CREATE DATABASE IF NOT EXISTS {}'.format(database),retry=True))


            self.mysql_cur.execute('SHOW CREATE TABLE {}.{}'.format(database,tablename))
            result = self.mysql_cur.fetchall()
            create_sql = result[0]['Create Table']

            self.__check_stat(self.__raise_sql(sql='USE {}'.format(database),retry=True))
            self.__check_stat(self.__raise_sql(sql='DROP TABLE IF EXISTS {}'.format(tablename),retry=True))
            self.__check_stat(self.__raise_sql(sql=create_sql,retry=True))
        return True

    def __column_join(self,cols,source=None):
        if self.destination_type in ('phoenix','postgresql'):
            if source:
                if self.queal_struct:
                    return ','.join(['`{}`'.format(cols[col]) for col in cols])
                return ','.join(['`{}`'.format(col) for col in cols])
            else:
                if self.queal_struct:
                    return ','.join(['{}'.format(cols[col]) for col in cols])
                return ','.join(['{}'.format(col) for col in cols])
        if self.queal_struct:
            return ','.join(['`{}`'.format(cols[col]) for col in cols])
        return ','.join(['`{}`'.format(col) for col in cols])


    def dump_to_new_db(self,database,tablename,idx,pri_idx,chunk_list=None,bytes_col_list=None,tbl=None,cols=None,iso=None):
        self.database = database
        iso_value = Iso(**{'iso': iso, 'conf_cols': cols})
        ''''''
        if tbl and cols:
            self.queal_struct = True
            _cols, _dcols, _ucols = iso_value.get_cols()
            _cols = ','.join(['`{}`'.format(c) for c in _cols])

        sql_type = 'INSERT'
        if self.destination_type in ('phoenix', 'postgresql'):
            sql_type = 'UPSERT'

        if tbl:
            if cols:
                if iso:
                    if self.destination_type == 'phoenix':
                        _cl = ','.join(['{}'.format(col) for col in _dcols] + ['deleted'])
                    else:
                        _cl = ','.join(['`{}`'.format(col) for col in _dcols])
                    insert_sql = '{} INTO {}.{}({}) VALUES'.format(sql_type, tbl[0], tbl[1], _cl)
                else:
                    insert_sql = '{} INTO {}.{}({}) VALUES'.format(sql_type, tbl[0], tbl[1], self.__column_join(cols))
            else:
                insert_sql = '{} INTO {}.{}({}) VALUES'.format(sql_type, tbl[0], tbl[1],
                                                               self.__column_join(self.table_column_struct))
        else:
            insert_sql = '{} INTO {}.{}({}) VALUES'.format(sql_type, database, tablename,
                                                           self.__column_join(self.table_column_struct))
        ''''''

        for cl in chunk_list:
            start_num = cl[0]
            end_num = cl[1]
            limit_num = 0


            while True:
                '''
                第一次使用分块大小limit N,M, N代表起始个数位置（chunks大小），M代表条数
                第一次执行之后获取最大主键或唯一索引值范围查找
                每个线程查询一次累加条数当剩余条数小于1000时调用__get_from_source_db_list
                每个chunk剩余条数大于1000固定调用__get_from_source_db_limit1000
                '''

                if tbl and cols:
                    sql = 'SELECT {} FROM {}.{} WHERE {}>=%s and {}<=%s ORDER BY {} LIMIT {},%s'.format(_cols,database, tablename,
                                                                                                   idx, idx, idx, limit_num)
                else:
                    sql = 'SELECT {} FROM {}.{} WHERE {}>=%s and {}<=%s ORDER BY {} LIMIT {},%s'.format(self.__column_join(self.table_column_struct,True),
                                                                                                        database, tablename,
                                                                                                   idx, idx, idx, limit_num)

                self.__get_from_source_db_limit2000(sql=sql, args_value=[start_num, end_num])
                '''======================================================================================================'''

                '''
                拼接行数据为pymysql格式化列表
                如果返回数据为空直接退出
                '''
                self.all_value = []

                if self.result:
                    _len = len(self.result[0])
                    _num = len(self.result)

                    ''''''


                    if self.destination_type == 'phoenix':
                        _valus_list = []
                        for row in self.result:
                            if self.queal_struct:
                                row = iso_value.iso_value(row)
                            _values = [_ for _ in row.values()]
                            _valus_list.append(self.escape_string(_values))
                        _len = len(_valus_list[0])
                        _sql = insert_sql + '{}'.format(self.__combination_value_format(_len=_len, _num=None))
                        self.__check_stat(self.__raise_sql(sql=_sql, args=_valus_list, retry=True))

                    else:
                        # for row in self.result:
                        #     if self.queal_struct:
                        #         row = iso_value.iso_value(row)
                        #     _values = [_ for _ in row.values()]
                        #     _len = len(_values)
                        #     _sql = insert_sql + '{}'.format(self.__combination_value_format(_len=_len, _num=None))
                        #     self.__check_stat(self.__raise_sql(sql=_sql, args=_values, retry=True))
                        #     self.__check_stat(self.__raise_sql(sql='commit'))

                        with_msgid = []

                        for row in self.result:
                            if self.queal_struct:
                                row = iso_value.iso_value(valus=row)
                                self.all_value += row.values()

                                continue
                            self.all_value += row.values()
                        if self.queal_struct:
                            _len = len(_dcols)
                        self.sql = insert_sql + '{}'.format(self.__combination_value_format(_len=_len,_num=_num))
                        self.__check_stat(self.__raise_sql(sql=self.sql,args=self.all_value))
                        self.__check_stat(self.__raise_sql(sql='commit'))

                else:
                    #Logging(msg='return value is empty',level='warning')
                    break

                '''
                每次循环结束计算该线程还剩未处理的条数（limit_num）
                当返回条数少于1000条时将退出整个循环
                '''
                return_len = len(self.result)
                limit_num += return_len
                if return_len < 2000:
                    break
                '''=========================================='''


    def __get_from_source_db_list(self,sql,pri_value=None):
        try:
            self.mysql_cur.execute(sql,pri_value)
            self.result = self.mysql_cur.fetchall()
        except pymysql.Error:
            Logging(msg=traceback.format_exc(),level='error')
            sys.exit()

    def __get_from_source_db_limit2000(self,sql,args_value):
        try:
            if args_value:
                #print(sql,args_value)
                self.mysql_cur.execute(sql, args_value + [2000])
            else:
                self.mysql_cur.execute(sql,2000)
            self.result = self.mysql_cur.fetchall()
        except pymysql.Error:
            Logging(msg=traceback.format_exc(),level='error')
            sys.exit()



    def __combination_value_format(self,_len,_num):
        '''拼接格式化字符'''
        if self.destination_type == 'phoenix':
            one_format = '({})'.format(','.join(['?' for i in range(_len)]))
        else:
            one_format = '({})'.format(','.join(['%s' for i in range(_len)]))
        if _num:
            all_ = ','.join(one_format for i in range(_num))
            return all_
        return one_format

    def __raise_sql(self, sql, args=[],retry=None):
        '''
        追加binlog数据到目标库
        :param sql:
        :param args:
        :return:
        '''
        try:
            args = self.escape_string(args) if args else []
        except:
            Logging(msg=traceback.format_exc(),level='error')
        try:
            if sql == 'commit':
                self.des_mysql_conn.commit()
            else:
                if self.destination_type == 'phoenix':
                    self.des_mysql_cur.executemany(sql,args)
                else:
                    self.des_mysql_cur.execute(sql, args)
        except phoenixdb.errors.InternalError:
            Logging(msg=traceback.format_exc(), level='error')
            self.__retry_execute(sql=sql,args=args,retry=retry)
            return True
        except psycopg2.OperationalError as e:
            Logging(msg=e.args, level='error')
            #Logging(msg='sql:{},values:{}'.format(sql, args), level='error')
            if sql == 'commit':
                self.__retry_execute(retry=retry)
            else:
                self.__retry_execute(sql=sql,args=args,retry=retry)
            return True
        except psycopg2.InterfaceError as e:
            Logging(msg=e.args, level='error')
            if sql == 'commit':
                self.__retry_execute(retry=retry)
            else:
                self.__retry_execute(sql=sql,args=args,retry=retry)
            return True
        except pymysql.Error as e:
            Logging(msg=e.args, level='error')
            if e.args[0] in ErrorCode:
                if ErrorCode[e.args[0]]:
                    if sql == 'commit':
                        self.__retry_execute(retry=retry)
                    else:
                        self.__retry_execute(sql=sql,args=args,retry=retry)
                    return True
            #Logging(msg='sql:{},values:{}'.format(sql, args), level='error')
            Logging(msg=e, level='error')
            return None
        except:
            #Logging(msg='sql:{},values:{}'.format(sql, args), level='error')
            Logging(msg=traceback.format_exc(), level='error')
            return None
        return True


    def __check_stat(self, state):
        if state:
                return
        else:
            Logging(msg='desthread failed!!!!', level='error')
            sys.exit()

    def __retry_execute(self,sql=None,args=None,retry=None):
        '''
        异常重试
        :param sql: sql语句
        :param args: 参数列表
        :param type: 报错时是否需要重新执行该sql
        :param retry: 需要全事务重新执行
        :return:
        '''
        self.__retry_connection_destion()
        if retry:
            self.__raise_sql(sql=sql, args=args, retry=retry)
            return
        else:
            #Logging(msg='sql={},args={},retry={},type={}'.format(sql, args,retry, type), level='info')
            Logging(msg='retry execute sql',level='info')
            self.__raise_sql(sql=self.sql, args=self.all_value,retry=True)
            self.__raise_sql('commit')
            return

    def __retry_connection_destion(self):
        '''
        目标库链接丢失重试60次，如果60次都失败将退出整个程序
        使用30次的原因是有可能目标数据在发生宕机切换，如果30
        秒都无法完成重连那表示数据库已经宕机或无法链接
        :return:
        '''
        import time
        if self.des_mysql_conn:
            try:
                Logging(msg='close destination db connection',level='info')
                self.des_mysql_conn.close()
                self.des_mysql_cur.close()
            except:
                Logging(msg=traceback.format_exc(),level='error')
        time.sleep(10)
        for i in range(60):
            Logging(msg='connection to destination db try agian!!!', level='info')
            if self.destination_type == 'phoenix':
                self.des_mysql_conn = InitPhoenixDB(host=self.des_conn_info['mysql_host'],
                                                    port=self.des_conn_info['mysql_port'],
                                                    user=self.des_conn_info['mysql_user'],
                                                    passwd=self.des_conn_info['mysql_password'],
                                                    jar=self.jar, jar_conf=self.jar_conf).Init()
                if self.des_mysql_conn:
                    try:
                        self.des_mysql_cur = self.des_mysql_conn.cursor()
                        return True
                    except:
                        Logging(msg=traceback.format_exc(), level='error')
            else:
                self.des_mysql_conn = InitMyDB(**dict(self.des_conn_info,**{'type':self.destination_type})).Init()
                if self.des_mysql_conn:
                    try:
                        self.des_mysql_cur = self.des_mysql_conn.cursor()
                        if self.destination_type and self.destination_type != 'mysql':
                            pass
                        else:
                            if self.binlog is None:
                                self.des_mysql_cur.execute('set sql_log_bin=0;')
                            self.des_mysql_cur.execute('SET SESSION wait_timeout = 2147483;')
                        return True
                    except:
                        Logging(msg=traceback.format_exc(), level='error')
            time.sleep(1)
        else:
            Logging(msg='try 60 times to fail for conncetion destination db,exist now', level='error')
            sys.exit()