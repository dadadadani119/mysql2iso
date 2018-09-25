# -*- coding: utf-8 -*-
''' 
@Time    : 2018/7/17 13:41
@Author  : Great God
@File    : escape.py
@Software: PyCharm
'''

import json

class escape:
    def __init__(self):
        '''
        格式化数据，转移特殊字符、格式化json
        '''
        pass

    def escape_string(self,value):
        if type(value) in (list,dict):
            _a = []
            for i in value:
                if type(i) in (list,dict):
                    _a.append(json.dumps(self.__escape_struct(i)))
                else:
                    _a.append(i)
            return _a
        elif type(value) is bytes:
            return value.decode()
        else:
            return value


    def __escape_struct(self,value):
        if type(value) is dict:
            _value = {}
            for v in value:
                if type(value[v]) in (list,dict):
                    _value[self.__escape_bytes(v)] = self.__escape_struct(value[v])
                elif type(value[v]) is bytes:
                    _value[self.__escape_bytes(v)] = value[v].decode()
                else:
                    _value[self.__escape_bytes(v)] = value[v]
        elif type(value) is list:
            _value = []
            for v in value:
                if type(v) in (list,dict):
                    _value.append(self.__escape_struct(v))
                elif type(v) is bytes:
                    _value.append(v.decode())
                else:
                    _value.append(v)
        return _value

    def __escape_bytes(self,value):
        if type(value) is bytes:
            return value.decode()
        else:
            return value

    def escape_args(self,args):
        if type(args) == list:
            _args = []
            for i in range(len(args)):
                if type(args[i]) in (dict,list):
                    _args.append("'{}'".format(json.dumps(args[i])))
                elif type(args[i]) ==str:
                    _args.append(self.escape_args(args[i]))
                elif type(args[i]) == int:
                    _args.append(args[i])
                else:
                    _args.append("'Null'")
            return _args
        elif type(args) ==str:
            if args in ('null','Null'):
                return "'Null'"
            else:
                if '\'' in args:
                    return "'{}'".format(args.replace('\'', '\\\''))

                return "'{}'".format(args)
        elif type(args) == int:
            return args
        else:
            return "'Null'"
