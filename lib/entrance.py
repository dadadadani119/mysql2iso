# -*- encoding: utf-8 -*-
'''
@author: Great God
'''
import sys
sys.path.append("..")
from binlog import Metadata
from .OperationDB import OperationDB


class Entrance(Metadata.TableMetadata):
    def __init__(self,kargs):
        self.queue = kargs['queue']
        self.binlog_file = kargs['binlog_file']
        self.start_position = kargs['start_position']
        self.gtid = kargs['gtid']
        self.auto_position = kargs['auto_position']

        self.host = kargs['host'] if kargs['host'] else '127.0.0.1'
        self.port = kargs['port']  if kargs['port'] else 3306
        self.user = kargs['user_name']
        self.passwd = kargs['user_password']
        self.socket = kargs['socket'] if kargs['socket'] else None

        self.databases = kargs['databases'].split(',')
        self.tables = kargs['tables'].split(',') if kargs['tables'] else None

        '''目标库信息'''
        self.d_host = kargs['dhost']
        self.d_port = kargs['dport']
        self.d_user = kargs['duser']
        self.d_passwd = kargs['dpassword']
        self.ignore_type = kargs['ignore'] if kargs['ignore'] else None
        self.server_id = kargs['server_id'] if kargs['server_id'] else 133
        self.binlog = kargs['binlog'] if kargs['binlog'] else None

        self.ithread = kargs['ignore_thread'] if kargs['ignore_thread'] else None
        self.full_dump = kargs['full'] if kargs['full'] else None
        self.threads = kargs['threads'] if kargs['threads'] else None

        '''ssl加密链接信息'''
        self.ssl = kargs['ssl']
        self.cert = kargs['cert']
        self.key = kargs['key']

        ''''''
        self.daemon = kargs['daemon']

        '''状态存储库信息'''
        self.shost = kargs['shost']
        self.sport = kargs['sport'] if 'sport' in kargs else 3306
        self.suser = kargs['suser']
        self.spassword = kargs['spassword']
        self.sbinlog = kargs['sbinlog'] if 'sbinlog' in kargs else False

    def __enter__(self):
        OperationDB(databases=self.databases,tables=self.tables,binlog_file=self.binlog_file,start_position=self.start_position,
                    host=self.host,port=self.port,user=self.user,passwd=self.passwd,dhost=self.d_host,dport=self.d_port,
                    duser=self.d_user,dpasswd=self.d_passwd,socket=self.socket,ignore_type=self.ignore_type,
                    server_id=self.server_id,binlog=self.binlog,full_dump=self.full_dump,threads=self.threads,
                    ithread=self.ithread,gtid=self.gtid,auto_position=self.auto_position,ssl=self.ssl,cert=self.cert,
                    key=self.key,daemon=self.daemon,shost=self.shost,sport=self.sport,suser=self.suser,
                    spassword=self.spassword,sbinlog=self.sbinlog,queue=self.queue).Operation()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

