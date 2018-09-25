# -*- coding: utf-8 -*-
''' 
@Time    : 2018/8/13 15:33
@Author  : Great God
@File    : GetPhoenixSql.py
@Software: PyCharm
'''

import sys
sys.path.append("..")
from lib.escape import escape
from binlog.Metadata import binlog_events
from lib.get_conf import GetStruct

class GetSql(escape):
    def __init__(self,table_pk_idex_list,table_struct_list,map_conf=None,queal_struct=None):
        super(GetSql,self).__init__()
        self.map_conf = map_conf
        self.queal_struct = queal_struct
        self._tmp_struct_list = []
        if self.queal_struct:
            self._tmp_struct_list = GetStruct(map_name=self.map_conf).get()

        self.table_struct_list = table_struct_list
        self.table_pk_idex_list = table_pk_idex_list



        self._tmp_col_struct = []   #临时记录当前异构表字段结构
        self._tmp_pri_struct = []        #当前异构表主键字段
        self._tmp_pri_idx = []


    def WhereJoin(self,table_struce_key):
        if self.queal_struct and self._tmp_col_struct:
            return ' AND '.join(['`{}`=%s'.format(col) for col in self._tmp_col_struct])
        return ' AND '.join(['`{}`=%s'.format(col) for col in self.table_struct_list[table_struce_key]])

    def SetJoin(self,table_struce_key):
        if self.queal_struct and self._tmp_col_struct:
            return ','.join(['`{}`=%s'.format(col) for col in self._tmp_col_struct])
        return ','.join(['`{}`=%s'.format(col) for col in self.table_struct_list[table_struce_key]])

    def ValueJoin(self,table_struce_key):
        if self.queal_struct and self._tmp_col_struct:
            return '({})'.format(','.join(['%s' for i in range(len(self._tmp_col_struct))]))
        return '({})'.format(','.join(['%s' for i in range(len(self.table_struct_list[table_struce_key]))]))

    def PkJoin(self,pk_list,table_struce_key):
        pk_col = []
        for pk in pk_list:
            pk_col.append(self.table_struct_list[table_struce_key][pk])
        if self.queal_struct:
            self.__equel_check_pri(table_struce_key,pk_col)
            if self._tmp_pri_struct:
                pk_col = self._tmp_pri_struct

        if len(pk_col) > 1:
            return ' AND '.join(['`{}`=%s'.format(col) for col in pk_col])
        else:
            return '`{}`=%s'.format(pk_col[0])

    def GetSQL(self,_values=None,event_code=None,database_name=None,table_name=None):
        table_struce_key = '{}:{}'.format(database_name,table_name)
        if self.queal_struct:
            self.__equel_check_col(database_name,table_name)
            database_name,table_name = self.__equel_table_name(database_name,table_name)
            if database_name is None or table_name is None:
                return None
        if self.table_pk_idex_list[table_struce_key]:
            '''获取主键所在index'''
            __pk_idx = self.table_pk_idex_list[table_struce_key]
            pk_where = self.PkJoin(__pk_idx,table_struce_key)
        else:
            __pk_idx = None

        if self.queal_struct and self._tmp_col_struct:
            __pk_idx = self._tmp_pri_idx

        _tmp_sql_list = []
        if event_code == binlog_events.UPDATE_ROWS_EVENT:
            __values = [_values[i:i + 2] for i in range(0, len(_values), 2)]
            for row_value in __values:
                if self.queal_struct:
                    row_value = self.__equel_check_value(row_value,table_struce_key,event_code)

                if __pk_idx is not None:
                    cur_sql = 'UPDATE {}.{} SET {} WHERE {}'.format(database_name, table_name,
                                                                       self.SetJoin(table_struce_key), pk_where)
                    pk_values = []
                    for i in __pk_idx:
                        pk_values.append(row_value[0][i])

                    _args = row_value[1] + pk_values
                else:
                    cur_sql = 'UPDATE {}.{} SET {} WHERE {}'.format(database_name, table_name,
                                                                   self.SetJoin(table_struce_key),
                                                                   self.WhereJoin(table_struce_key))
                    _args = row_value[1] + row_value[0]
                _tmp_sql_list.append([cur_sql,_args])

        else:
            if event_code == binlog_events.WRITE_ROWS_EVENT:
                values_str = ','.join([self.ValueJoin(table_struce_key) for i in range(len(_values))])
                if self.queal_struct and self._tmp_col_struct:
                    col = '({})'.format(','.join(['`{}`'.format(i) for i in self._tmp_col_struct]))
                else:
                    col = '({})'.format(','.join(['`{}`'.format(i) for i in self.table_struct_list[table_struce_key]]))
                # cur_sql = 'INSERT INTO {}.{} VALUES{};'.format(tmepdata.database_name, tmepdata.table_name,
                #                                                    values_str)
                cur_sql = 'INSERT INTO {}.{}{} VALUES{};'.format(database_name, table_name,
                                                                   col,values_str)
                all_values = []
                for value in _values:
                    if self.queal_struct:
                        value = self.__equel_check_value(value,table_struce_key,event_code)
                    all_values += value
                _tmp_sql_list.append([cur_sql,all_values])
            elif event_code == binlog_events.DELETE_ROWS_EVENT:
                for value in _values:
                    if self.queal_struct:
                        value = self.__equel_check_value(value,table_struce_key,event_code)
                    if __pk_idx is not None:
                        cur_sql = 'DELETE FROM {}.{} WHERE {};'.format(database_name,table_name,pk_where)
                        pk_values = []
                        for i in __pk_idx:
                            pk_values.append(value[i])
                        _args = pk_values
                    else:
                        cur_sql = 'DELETE FROM {}.{} WHERE {};'.format(database_name,table_name,self.WhereJoin(table_struce_key))
                        _args = value
                    _tmp_sql_list.append([cur_sql,_args])
        self._tmp_col_struct = []
        self._tmp_pri_struct = []
        self._tmp_pri_idx = []
        return _tmp_sql_list

    def __equel_table_name(self,database_name,table_name):
        '''
        获取异构对应的库、表名称
        :param database_name:
        :param table_name:
        :return:
        '''
        if database_name in self._tmp_struct_list:
            if table_name in self._tmp_struct_list[database_name]:
                tbl = self._tmp_struct_list[database_name][table_name]['tbl']
                return tbl[0],tbl[1]
        return None,None

    def __equel_check_value(self,_value,db_tb,code):
        '''
        对数据进行异构对应，删除不需要的字段值
        :param _value:
        :param database_name:
        :param table_name:
        :return:
        '''
        __i = db_tb.split(':')
        database_name, table_name = __i[0], __i[1]
        if database_name in self._tmp_struct_list:
            if table_name in self._tmp_struct_list[database_name]:
                source_struct = self.table_struct_list[db_tb]
                if 'cols' in self._tmp_struct_list[database_name][table_name]:
                    _cols = self._tmp_struct_list[database_name][table_name]['cols']
                    for idx,_c in enumerate(source_struct):
                        if _c not in _cols:
                            if code == binlog_events.UPDATE_ROWS_EVENT:
                                _value[0].pop(idx)
                                _value[1].pop(idx)
                            else:
                                _value.pop(idx)
                return _value
            else:
                return None
        else:
            return None

    def __equel_check_col(self,database_name,table_name):
        '''
        获取异构字段列表
        :param database_name:
        :param table_name:
        :return:
        '''
        if database_name in self._tmp_struct_list:
            if table_name in self._tmp_struct_list[database_name]:
                if 'cols' in self._tmp_struct_list[database_name][table_name]:
                    _cols = self._tmp_struct_list[database_name][table_name]['cols']
                    _cols = [[_c, _cols[_c]] for _c in _cols]
                    col_names = []
                    for i in range(len(_cols)):
                        col_names.append(_cols[i][1])

                    self._tmp_col_struct = col_names



    def __equel_check_pri(self,db_tb,pk_col):
        '''
        获取主键索引异构字段及idex
        :param database_name:
        :param table_name:
        :param pk_col:
        :return:
        '''
        __i = db_tb.split(':')
        database_name,table_name = __i[0],__i[1]
        if database_name in self._tmp_struct_list:
            if table_name in self._tmp_struct_list[database_name]:
                if 'cols' in self._tmp_struct_list[database_name][table_name]:
                    _cols = self._tmp_struct_list[database_name][table_name]['cols']
                    _cols = [[_c, _cols[_c]] for _c in _cols]
                    for col in pk_col:
                        for idx,i in enumerate(_cols):
                            if i[0] == col:
                                self._tmp_pri_struct.append(i[1])
                                self._tmp_pri_idx.append(idx)






