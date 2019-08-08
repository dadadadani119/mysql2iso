#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@author: xiao cai niao
'''
import time

class manager:
    def __init__(self,put_task_list,master_thread_list,client,cmd):
        """
        管理socket接受到请求
        :param put_task_list:
        :param master_thread_list:
        :param client:
        :param cmd:
        """
        self.put_task_list = put_task_list
        self.master_thread_list = master_thread_list
        self.client = client
        self.case_list = {'put':self.put,'get':self.get,'set':self.set,'other':None}
        self.cmd = cmd
        self.cmd_len = len(cmd)

    def put(self):
        """
        新增操作
        :return:
        """
        if self.cmd_len == 2:
            try:
                put_value = eval(self.cmd[1])
            except:
                self.client.send('command error '.encode())
                return
            if 'task_name' in put_value and 'config' in put_value and put_value['task_name'] not in (row['task_name'] for row in self.master_thread_list):
                task_cmd = eval(self.cmd[1])
                if 'state' not in put_value:
                    task_cmd['state'] = 'nil'
                else:
                    if self.check(key='state',value=task_cmd['state']) is None:
                        return
                task_cmd['start_time'] = ''
                task_cmd['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                self.put_task_list.append(task_cmd)
                self.client.send('ok ....'.encode())
            else:
                self.client.send('error, duplicate key for task_name or is nil'.encode())
        else:
            self.client.send('command error '.encode())

    def get(self):
        """
        查询操作
        :return:
        """
        if 1 < self.cmd_len < 4:
            _v = [row for row in self.master_thread_list if row[self.cmd[1]] == self.cmd[2]]
            if _v:
                self.client.send(str(_v[0]).encode())
            else:
                self.client.send('None'.encode())
        elif 0 < self.cmd_len < 3:
            if self.master_thread_list:
                self.client.send(str(self.master_thread_list).encode())
            else:
                self.client.send('None'.encode())
        else:
            self.client.send('command error '.encode())

    def set(self):
        """
        修改数据类操作
        :return:
        """
        if 3 < self.cmd_len < 6:
            set_key, set_value = self.cmd[1], self.cmd[2]
            where_key, where_value = self.cmd[3], self.cmd[4]
            if self.check(key=set_key,value=set_value) is None:
                return
            if set_key in ('config','state'):
                for idx, row in enumerate(self.master_thread_list):
                    if where_key in row:
                        if row[where_key] == where_value:
                            if row[set_key] != set_value:
                                row[set_key] = set_value
                                self.master_thread_list[idx] = row.copy()
                                self.client.send('command ok'.encode())
                            else:
                                self.client.send('this set value not variety '.encode())
                else:
                    self.client.send('where key not found '.encode())
            else:
                self.client.send('only support modifications config or state '.encode())
        elif 1 < self.cmd_len < 4:
            set_key, set_value = self.cmd[1], self.cmd[2]
            for idx, row in enumerate(self.master_thread_list):
                if set_key in row:
                    row[set_key] = set_value
                    self.master_thread_list[idx] = row.copy()
            self.client.send('command ok'.encode())
        else:

            self.client.send('command error '.encode())

    def other(self):
        self.client.send('command only suppor get,set,put '.encode())

    def check(self,key=None,value=None):
        if key == 'state' and value not in ('started', 'stoped'):
            self.client.send('initial state value error,only support started or stoped '.encode())
            return None
        return True