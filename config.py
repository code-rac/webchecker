# Load connect to database

import pymysql
from elasticsearch import Elasticsearch
import threading
import time

HOST = '192.168.1.65'
N_MYSQL_CONNECTIONS = 30
N_ES_CONNECTIONS = 100
MYSQL_LOCK = threading.Lock()
ES_LOCK = threading.Lock()


class Config:

    def __init__(self):
        self.reload()

    def reload(self):
        self.mysql_connections = []
        for c in range(N_MYSQL_CONNECTIONS):
            try:
                mysql_conn = pymysql.connect(
                    host=HOST, user='root', password='vnistadmin', db='webix', charset='utf8mb4')
                mysql_cur = mysql_conn.cursor()
            except:
                time.sleep(1)
                print(time.ctime(time.time())),
                print('mysql connection error')
            else:
                self.mysql_connections.append((mysql_conn, mysql_cur))

        # Elasticsearch
        self.elasticsearch_connections = []
        for c in range(N_ES_CONNECTIONS):
            try:
                es = Elasticsearch(['%s:9200' % HOST])
            except:
                time.sleep(1)
                print(time.ctime(time.time())),
                print('elasticsearch connection error')
            else:
                self.elasticsearch_connections.append(es)

    def get_mysql(self):
        while 1:
            MYSQL_LOCK.acquire()
            if self.mysql_connections:
                conn, cur = self.mysql_connections.pop(0)
                MYSQL_LOCK.release()
                return conn, cur
            MYSQL_LOCK.release()
            time.sleep(0.1)

    def get_es(self):
        while 1:
            ES_LOCK.acquire()
            if self.elasticsearch_connections:
                es = self.elasticsearch_connections.pop(0)
                ES_LOCK.release()
                return es
            ES_LOCK.release()
            time.sleep(0.1)

    def append_mysql(self, conn, cur):
        MYSQL_LOCK.acquire()
        self.mysql_connections.append((conn, cur))
        MYSQL_LOCK.release()

    def append_es(self, es):
        ES_LOCK.acquire()
        self.elasticsearch_connections.append(es)
        ES_LOCK.release()

if __name__ == '__main__':
    c = Config()
