# -*- encoding: utf-8 -*-
'''
@author: xiao cai niao
'''


import configparser
import sys
from .Loging import Logging
import os
path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

class GetConf(object):
    '''获取配置项'''
    def __init__(self,conf_name):
        conf_path = path.replace('\\','/') +'/conf/include/{}'.format(conf_name)
        self.global_s = 'global'
        self.source = 'source'
        self.destination = 'destination'
        self.status = 'status'
        self.attach = 'attach'
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
            'ignore':[],                 #过滤操作，可选[delete,insert,update]
            'ignore_thread':False,          #过滤线程ID产生的binlog
            'ssl':False,                    #ssl链接
            'cert':None,
            'key':None,
            'daemon':None,                  #重启操作
            'destnation_type':'mysql',         #目标库类型
            'queal_struct':False,            #同构、异构
            'map_conf':None ,                #异构对应关系
            'lookback':False                #回环控制
        }
        global_options = self.conf.options(self.global_s)
        for option in global_options:
            if option in options_conf:
                option_value = self.conf.get(self.global_s, option)
                if option in ('full','ssl','daemon','queal_struct','lookback'):
                    options_conf[option] = True if option_value == 'True' else False
                elif option in ('threads','server_id','ignore_thread'):
                    options_conf[option] = int(option_value) if option_value else None
                elif option == 'destnation_type':
                    if options_conf[option] in ('mysql','phoenix'):
                        options_conf[option] = option_value
                    else:
                        Logging(msg='invalid option {} ,useing default config'.format(option), level='warning')
                elif option == 'ignore':
                    options_conf[option] = option_value.split(',') if option_value else []
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
            'protocol' : None,                    #本地时使用socket链接
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
            'dthreads' : 1,                      #入库线程数
            'dhost' : None,                     #目标库地址
            'dport' : 3306,                     #目标库端口，默认3306
            'duser' : None,                     #目标库用户名
            'dpassword' : None,                 #目标库用户密码
            'binlog' : False,                   #同步数据是否记录到binlog,默认不记录
            'jar' : None,                       #phoenix使用jar包地址
            'jar_conf' : {}                     #phoenix配置参数列表
        }
        des_options = self.conf.options(self.destination)
        for option in des_options:
            if option in options_conf:
                option_value  = self.conf.get(self.destination,option)
                if option in 'dport':
                    options_conf[option] = int(option_value) if option_value else 3306
                elif option == 'binlog':
                    options_conf[option] = True if option_value == 'True' else False
                elif option == 'jar_conf':
                    options_conf[option] = eval(option_value)
                elif option == 'dthreads':
                    options_conf[option] = int(option_value)
                else:
                    options_conf[option] = option_value if option_value else None
            else:
                Logging(msg='invalid option {}'.format(option), level='warning')

        #if options_conf['dhost'] is None or options_conf['duser'] is None or options_conf['dpassword'] is None:
        if options_conf['dhost'] is None or options_conf['duser'] is None:
            Logging(msg='{} cannot is null'.format('dhost/duser/dpassword'), level='error')
            sys.exit()

        return options_conf

    def GetStatus(self):
        '''
        获取所有目标库配置项
        :return:
        '''
        options_conf = {
            'shost' : None,                     #目标库地址
            'sport' : 3306,                     #目标库端口，默认3306
            'suser' : None,                     #目标库用户名
            'spassword' : None,                 #目标库用户密码
            'sbinlog' : False                   #同步数据是否记录到binlog,默认不记录
        }
        des_options = self.conf.options(self.status)
        for option in des_options:
            if option in options_conf:
                option_value  = self.conf.get(self.status,option)
                if option == 'sport':
                    options_conf[option] = int(option_value) if option_value else 3306
                elif option == 'sbinlog':
                    options_conf[option] = True if option_value == 'True' else False
                else:
                    options_conf[option] = option_value if option_value else None
            else:
                Logging(msg='invalid option {}'.format(option), level='warning')

        if options_conf['shost'] is None or options_conf['suser'] is None or options_conf['spassword'] is None:
            Logging(msg='{} cannot is null'.format('shost/suser/spassword'), level='error')
            sys.exit()

        return options_conf

    def GetAttact(self):
        '''
        获取附加项配置
        :return:
        '''
        options_conf = {
            'ignores' : None                      #针对单独的库/表过滤操作
        }
        att_opptions = self.conf.options(self.attach)
        for option in att_opptions:
            if option in options_conf:
                options_conf[option] = self.conf.get(self.attach,option)
            else:
                Logging(msg='invalid option {}'.format(option), level='warning')
        return options_conf


class GetIso:
    def __init__(self):
        conf_path = path.replace('\\', '/') + '/conf/iso.conf'
        self.options = 'iso'
        self.conf = configparser.ConfigParser()
        self.conf.read(conf_path, encoding="utf-8-sig")

    def get(self):
        '''
        获取根配置文件配置项
        :return:
        '''
        option_conf = {
            'include' : None,                   #引用配置文件目录
            'config' : None,                    #单机模式下要执行的同步任务配置文件路径
            'cluster' : False,                  #是否集群模式，默认False
            'cluster_type' : 'zk_mode',         #集群模式类型,默认基于zk实现
            'cluster_nodes' : None,             #集群模式下配置的节点信息
            'zk_hosts' : None,                  #zk集群模式下需要的集群地址
            'server_port': 9898,                 #服务监听端口
            'host' : None                       #本机IP
        }

        conf_options = self.conf.options(self.options)
        for option in conf_options:
            if option in option_conf:
                option_value = self.conf.get(self.options,option)
                if option == 'cluster':
                    option_conf['cluster'] = True if option_value == 'True' else False
                elif option == 'server_port':
                    option_conf['server_port'] = int(option_value)
                else:
                    option_conf[option] = option_value
            else:
                Logging(msg='invalid option {}'.format(option), level='warning')
        if not option_conf['cluster'] and not option_conf['config']:
            Logging(msg='{} cannot is null, when cluster is False'.format('config'), level='error')

        return option_conf



class GetStruct:
    def __init__(self,map_name):
        conf_path = path.replace('\\', '/') + '/map/{}'.format(map_name)
        self.conf = configparser.ConfigParser()
        self.conf.read(conf_path, encoding="utf-8-sig")

    def get(self):
        dbs = self.conf.options('source_db')
        struct_list = {}
        for db in dbs:
            struct_list[db]=eval(self.conf.get('source_db', db))
        return struct_list

#get = GetConf()
#print(get.GetGlobal())
#print(get.GetSource())
#print(get.GetDestination())
#print GetConf().GetSlaveSSLCa()
#print GetConf().GetZKHosts()
