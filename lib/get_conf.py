# -*- encoding: utf-8 -*-
'''
@author: Great God
'''


import configparser
import sys
from .Loging import Logging
import os
path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

class GetConf(object):
    '''获取配置项'''
    def __init__(self):
        conf_path = path.replace('\\','/') +'/conf/iso.conf'
        self.keys_path = path.replace('\\','/')+'/conf/keys/'
        self.global_s = 'global'
        self.source = 'source'
        self.destination = 'destination'
        self.conf = configparser.ConfigParser()
        self.conf.read(conf_path,encoding="utf-8-sig")

    def GetGlobal(self):
        '''
        读取配置文件中global选项下所有配置
        :return:
        '''
        options_conf = {
            'server_id':133,                #slave线程serverid
            'full':False,                   #是否全量导出
            'threads':1,                    #全量到处时线程数
            'ignore':False,                 #过滤操作，可选[delete,insert,update]
            'ignore_thread':False,          #过滤线程ID产生的binlog
            'ssl':False,                    #ssl链接
            'cert':None,
            'key':None,
            'daemon':None                   #重启操作
        }
        global_options = self.conf.options(self.global_s)
        for option in global_options:
            if option in options_conf:
                option_value = self.conf.get(self.global_s, option)
                if option in ('full','ssl','daemon'):
                    options_conf[option] = True if option_value == 'True' else False
                elif option in ('threads','server_id','ignore_thread'):
                    options_conf[option] = int(option_value) if option_value else None
                else:
                    options_conf[option] = option_value if option_value else None
            else:
                Logging(msg='invalid option {}'.format(option),level='warning')
        return options_conf

    def GetSource(self):
        '''
        返回所有源库配置相关项
        :return:
        '''
        options_conf = {
            'host' : '127.0.0.1',               #源库地址
            'port' : 3306,                      #源库端口，默认3306
            'socket' : None,                    #本地时使用socket链接
            'user_name' : None,                 #源库用户名
            'user_password' : None,             #源库密码
            'databases' : None,                 #源库需要同步的数据库列表
            'tables': None,                     #源库同步的表
            'binlog_file' : None,               #开始同步的binlog
            'start_position' : None,            #binlog position
            'auto_position' : False,            #gtid模式，默认binlog模式
            'gtid' : None
        }
        source_options = self.conf.options(self.source)
        for option in source_options:
            if option in options_conf:
                option_value = self.conf.get(self.source, option)
                if option == 'port':
                    options_conf[option] = int(option_value) if option_value else 3306
                elif option == 'auto_position':
                    options_conf[option] = True if option_value == 'True' else False
                else:
                    options_conf[option] = option_value if option_value else None
            else:
                Logging(msg='invalid option {}'.format(option), level='warning')

        if options_conf['databases'] is None:
            Logging(msg='{} cannot is null'.format('databases'), level='error')
            sys.exit()
        if options_conf['user_name'] is None or options_conf['user_password'] is None:
            Logging(msg='{} cannot is null'.format('user_name/user_password'), level='error')
            sys.exit()
        return options_conf

    def GetDestination(self):
        '''
        获取所有目标库配置项
        :return:
        '''
        options_conf = {
            'dhost' : None,                     #目标库地址
            'dport' : 3306,                     #目标库端口，默认3306
            'duser' : None,                     #目标库用户名
            'dpassword' : None,                 #目标库用户密码
            'binlog' : False                    #同步数据是否记录到binlog,默认不记录
        }
        des_options = self.conf.options(self.destination)
        for option in des_options:
            if option in options_conf:
                option_value  = self.conf.get(self.destination,option)
                if option == 'dport':
                    options_conf[option] = int(option_value) if option_value else 3306
                elif option == 'binlog':
                    options_conf[option] = True if option_value == 'True' else False
                else:
                    options_conf[option] = option_value if option_value else None
            else:
                Logging(msg='invalid option {}'.format(option), level='warning')

        if options_conf['dhost'] is None or options_conf['duser'] is None or options_conf['dpassword'] is None:
            Logging(msg='{} cannot is null'.format('dhost/duser,dpassword'), level='error')
            sys.exit()

        return options_conf

#get = GetConf()
#print(get.GetGlobal())
#print(get.GetSource())
#print(get.GetDestination())
#print GetConf().GetSlaveSSLCa()
#print GetConf().GetZKHosts()
