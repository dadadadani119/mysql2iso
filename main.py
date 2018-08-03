# -*- encoding: utf-8 -*-
'''
@author: Great God
'''
import os
from lib import get_conf
from lib import SingeTask
from cluster import InitCluster
from lib.Loging import Logging
import queue

my_queue = queue.Queue(2048)

def init():
    _get = get_conf.GetIso()
    _argv = _get.get()
    if _argv['cluster']:
        path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
        file_list = []
        for root, dirs, files in os.walk('{}/conf/include'.format(path)):
            for file in files:
                if file.split('.')[-1] == 'conf':
                    file_list.append(file)

        task_list = {}
        for file in file_list:
            task_list[file.split('.')[0]] = start(file)
        if _argv['cluster_type'] == 'zk_mode':


            with InitCluster.ClusterEnt(**dict(task_list,**{'zk_hosts':_argv['zk_hosts'],
                                                            'cluster_type':_argv['cluster_type']})):
                pass
        elif _argv['cluster_type'] == 'leader_mode':
            with InitCluster.ClusterEnt(**dict(task_list,**{'cluster_nodes':_argv['cluster_nodes'],
                                                            'cluster_type': _argv['cluster_type'],
                                                            'self_host':_argv['host']})):
                pass
        else:
            Logging(msg='invalid option cluster_type {}'.format(_argv['cluster_type']), level='warning')
    else:
        with SingeTask.SingeTask(**(start(_argv['config']))):
            pass


def start(conf_name):
    _get = get_conf.GetConf(conf_name)
    _argv = {}
    _argv = dict(_argv,**dict(_get.GetGlobal(),**dict(_get.GetSource(),**dict(_get.GetDestination(),**_get.GetStatus()))))
    return _argv



if __name__ == "__main__":
    init()

