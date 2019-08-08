#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@author: xiao cai niao
'''

class Iso:
    def __init__(self,**kwargs):
        #self.scols = kwargs['scols']               #源表查询字段
        self.dcols = []                             #目标表需要写入的字段
        self.iso = kwargs['iso']                    #异构对应字段
        self.conf_cols = kwargs['conf_cols']        #配置文件中字段对应关系（非合并字段对应关系）

    def iso_value(self,valus):
        '''
        按异构对应字段组织数据
        :param valus:
        :return:
        '''

        _dv = {}
        for dcol in self.dcols:
            _dv[dcol] = None


        for scol in valus:
            if scol in self.conf_cols:
                _dv[self.conf_cols[scol]] = valus[scol]
        if self.iso:
            iso_cols = self.iso['col_iso']
            iso_str = self.iso['str_iso']
            for iso_col in iso_cols:
                _tmp = eval(iso_col)
                _tmp_value = []
                for _c in _tmp:
                    _tmp_value.append(str(valus[_c]))

                _dv[iso_cols[iso_col]] = '{}'.format(iso_str).join(_tmp_value)



        return _dv

    def get_cols(self):
        '''
        格式化查询、插入字段列表
        :param cols:
        :param iso:
        :return:
        '''
        _scols = [_c for _c in self.conf_cols]
        self.dcols = [self.conf_cols[_c] for _c in self.conf_cols]
        _ucols = {}
        if self.iso:
            if 'col_iso' in self.iso and self.iso['col_iso']:
                _ucols = self.iso['col_iso']
                for _a in _ucols:
                    for _c in eval(_a):
                        if _c not in _scols:
                            _scols.append(_c)
                    if _ucols[_a] not in self.dcols:
                        self.dcols.append(_ucols[_a])

        return _scols,self.dcols,_ucols
