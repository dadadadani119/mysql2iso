# -*- coding: utf-8 -*-
''' 
@Time    : 2018/6/28 15:25
@Author  : xiao cai niao
@File    : LeaderVote.py
@Software: PyCharm
'''
"""
该文件适用于leader选举等方法
"""
import IPy
import numpy as np

class LeaderVote(object):
    def __init__(self):
        self.global_leader_status = {}          #leader收到的记录

    def vote(self,nodes,task_name=None,global_status=None):
        if global_status:
            pass
        elif self.global_leader_status:
            pass
        else:
            '''初次选举'''
            vote_list = self.__nodevote(nodes=nodes)
            return vote_list


    def __nodevote(self,nodes):
        '''
        进行选举，IP转整数之后利用基数排序法选举最小位作为leader
        :param nodes:
        :return:
        '''
        nodes = [IPy.IP(node).int() for node in nodes]
        n = 1                                               # 最小的位数置为1（包含0）
        max = np.max(nodes)                                 # 得到带排序数组中最大数
        while max / (10 ** n) > 0:                          # 得到最大数是几位数
            n += 1
        for i in range(n):
            bucket = {}                                     # 用字典构建桶
            for x in range(10):
                bucket.setdefault(x, [])                    # 将每个桶置空
            for x in nodes:                                 # 对每一位进行排序
                radix = (int(x / (10 ** i))) % 10           # 得到每位的基数
                bucket[radix].append(x)                     # 将对应的数组元素加入到相应位基数的桶中
            j = 0
            for k in range(10):
                if len(bucket[k]) != 0:                     # 若桶不为空
                    for y in bucket[k]:                     # 将该桶中每个元素
                        nodes[j] = y                        # 放回到数组中
                        j += 1
        return [IPy.IP(node) for node in nodes]



