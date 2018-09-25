# -*- coding: utf-8 -*-
''' 
@Time    : 2018/6/29 13:10
@Author  : Great God
@File    : t3.py
@Software: PyCharm
'''
'''
描述（表示没接触过，第一次听说）
基数排序（radix sort）属于“分配式排序”（distribution sort），又称“桶子法”（bucket sort）或bin sort，顾名思义，它是透过键值的部份资讯，
将要排序的元素分配至某些“桶”中，藉以达到排序的作用，基数排序法是属于稳定性的排序，其时间复杂度为O (nlog(r)m)，
其中r为所采取的基数，而m为堆数，在某些时候，基数排序法的效率高于其它的稳定性排序法。
'''
import numpy as np
def RadixSort(a):
  n = 1                      #最小的位数置为1（包含0）
  max = np.max(a)            #得到带排序数组中最大数
  while max/(10**n) > 0:       #得到最大数是几位数
    n += 1
  for i in range(n):
    bucket = {}               #用字典构建桶
    for x in range(10):
      bucket.setdefault(x, [])  #将每个桶置空
    for x in a:                #对每一位进行排序
      radix =(int(x / (10**i))) % 10  #得到每位的基数
      bucket[radix].append(x) #将对应的数组元素加入到相应位基数的桶中
    j = 0
    for k in range(0, 10):
      if len(bucket[k]) != 0:    #若桶不为空
        for y in bucket[k]:     #将该桶中每个元素
          a[j] = y            #放回到数组中
          j += 1
if __name__ == '__main__':
  a = np.random.randint(0, 1000, size = 10)
  print("Before sorting...")
  print("---------------------------------------------------------------")
  print(a)
  print("---------------------------------------------------------------")
  RadixSort(a)
  print("After sorting...")
  print("---------------------------------------------------------------")
  print(a)
  print("---------------------------------------------------------------")