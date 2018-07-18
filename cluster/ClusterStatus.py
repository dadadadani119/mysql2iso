# -*- coding: utf-8 -*-
''' 
@Time    : 2018/6/28 15:23
@Author  : Great God
@File    : ClusterStatus.py
@Software: PyCharm
'''

'''
该文件负责处理集群状态字典信息维护，节点状态变更修改status文件
'''
import os
from .LeaderVote import LeaderVote

class ClusterStatus(LeaderVote):
    def __init__(self):
        super(ClusterStatus,self).__init__()
        self.path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
        self.status_path = self.path.replace('\\','/') +'/{}'.format('cluster_status')
        self.global_status = {}               #全局节点状态信息，实时维护并写入cluster_status文件

    def checkstatus(self):
        if os.path.exists(self.status_path):
            status_list = []
            with open(self.status_path,'r') as f:
                for l in f.readlines():
                    status_list.append(eval(l.strip()))
            for row in status_list:
                if 'host' in row:
                    _host = row['host']
                    del row['host']
                    self.global_status[_host] = row
        else:
            return None


