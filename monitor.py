import json
import time
import os
import socket
import requests
import logging
from rediscluster import RedisCluster
from redis import Redis
from logging.handlers import TimedRotatingFileHandler
class Notify(object):
    def __init__(self):
        pass
    def _tcpconnect(self,host,port):
        # 建立连接:
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.connect((host, port))
        except ConnectionError:
            return None
        return  self._sock
    def send_to_sms(self,hosts,data):
        for host in hosts:
            print('ip:',host['host'],'port',host['port'])
            context=self._tcpconnect(host['host'],host['port'])
            if context!=None:
                context.send(bytes(data))
                context.close()
                break
        else:
            print('connect to sms error')


    def send_to_wechat(self):
        pass

    def send_toserver(self):
        pass


class RedisMonitor(object):

    def __init__(self, nodes):
        self._cluster = RedisCluster(startup_nodes=nodes, decode_responses=True)
        self.node_dict = self._cluster.info()

    def getclusterstate(self):
        clusterinfo = self._cluster.cluster_info()
        err_node ={}
        for ip, info in clusterinfo.items():
            if (info['cluster_state'] != 'ok'):
                info['role']=self.node_dict[ip]['role']
                err_node[ip]=info
            else:
                normal_info={}
                normal_info['role']=self.node_dict[ip]['role']
                normal_info['cluster_state']=info['cluster_state']
                err_node[ip]=normal_info
        return err_node

    # 获取节点关系，判断是否发生主从切换
    def getrelationship(self):
        err_node={}
        for ip, node_info in self._cluster.info(section='Replication').items():
            if (self.node_dict[ip]['role'] != node_info['role']):
                err_node[ip]=self.node_dict[ip]['role']+'============>'+node_info['role']
        return err_node

    def getcpuinfo(self):
        err_node = {}
        # for ip, node_info in self._cluster.info(section='CPU').items():
        #     err_node[ip] = self.node_dict[ip]['used_cpu_sys']
        return err_node
    def flushdb(self):
        self._cluster.flushall()
    def getmeminfo(self):
        err_node = {}
        for ip, node_info in self._cluster.info(section='Memory').items():
            err_node[ip] = self.node_dict[ip]['used_memory_human']
        return  err_node

def config_log():
    rq = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))

    if os.path.exists('Logs')==False:
        os.mkdir('Logs')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Log等级总开关
    formatter=logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')
    fh = TimedRotatingFileHandler(filename='Logs/monitor.log', when='D',interval=1,backupCount=30)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

def main():
    node= Redis(host='10.126.27.58',port=9000)
    cluster_dict={}
    with open(file='config.json',mode='r') as fp:
       nodes_dict=json.load(fp=fp)
    #nodes=[{'host':'10.126.27.58','port':9004},{'host':'10.126.27.59','port':9000},{'host':'10.126.27.57','port':9000}]
    for cluster_name, node_list in nodes_dict.items():
        try:
            cluster=RedisMonitor(node_list)
        except Exception:
            print('connect error')
            cluster=None
        finally:
            cluster_dict[cluster_name]=cluster
    notify=Notify()
    #notify.send_to_sms([{'host':'127.0.0.1','port':6666}],'12345')
    while True:
        for cluster_name, monitor in cluster_dict.items():
            if monitor!=None:
                err_list ={}
                print(cluster_name+'*'*20)
                logger.info(cluster_name+'*'*20)
                err=monitor.getclusterstate()
                if len(err):
                    err_list["NODE STATE"]=err
                #print(json.dumps( monitor.getclusterstate(),indent=4))
                err=monitor.getrelationship()
                if len(err):
                    err_list["MASTER-SLAVE"]=err

                err=monitor.getcpuinfo()
                if len(err):
                    err_list["CPU"]=err
                err=monitor.getmeminfo()
                if len(err):
                    err_list["MEMORY"] = err
                    monitor.flushdb()
                if len(err_list):
                    err_json=json.dumps(err_list,indent=4)
                    print(err_json)
                    logger.info(err_json)
                    #notify.send_to_sms([{'host': '127.0.0.1', 'port': 43211}],err_json.encode(encoding='utf8'))

        time.sleep(240)

if __name__ == '__main__':
    logger=config_log()
    main()