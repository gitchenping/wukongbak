import os
import requests
import json
from clickhouse_driver import Client,connect
import pymssql
import pymongo
import pymysql
import redis
import happybase
from .decorate import loadenv,loaddbenv
import configparser
from sshtunnel import SSHTunnelForwarder
from utils.load import readconfini

if os.name == "posix":
    from pyhive import hive

def close_db(conn, cursor):
    """关闭数据库"""
    cursor.close()
    conn.close()

@loadenv(db='db_ck')
def connect_clickhouse(host=None, port=None, user=None, password=None, database=None, collection=None):
    conn = connect(host=host, port=port, user=user, password=password, database=database)
    # conn = connect(host=host,user=user, password=password, database=database)
    return conn.cursor()


@loadenv(db='db_ck')
def client_ck(host=None, port=None, user=None, password=None, database=None, collection=None):
    """链接ck数据库"""
    conn = Client(host=host, port=port, user=user, password=password, database=database)
    return conn


def connect_sqlserver(db):
    """连接sql server数据库"""
    connect = pymssql.connect('10.255.254.194', 'readuser', 'password', db)
    cursor = connect.cursor()
    return connect, cursor


@loadenv(db='db_hive')
def connect_hive(host=None, port=None, user=None, password=None, database=None, collection=None):
    # Password should be set if and only if in LDAP or CUSTOM mode; Remove password or use one of those modes
    conn = hive.Connection(host=host, port=port, username=user, database=database)
    return conn.cursor()


@loadenv(db='db_mysql')
def connect_mysql(host=None, port=None, user=None, password=None, database=None, collection=None):
    conn = pymysql.connect(host=host, port=port, user=user, password=password, db=database)
    return conn.cursor()


# 连接mongodb
@loadenv(db='db_mongo')
def connect_mongodb(host=None, port=None, user=None, password=None, database=None, collection=None):
    """链接mongodb"""
    client = pymongo.MongoClient("mongodb://" + host + ":" + port + "/")
    client.admin.authenticate(user, password, mechanism='SCRAM-SHA-1')

    db = client[database]
    coll = db[collection]
    return coll


@loadenv(db='db_redis')
def get_redis(host=None, port=None, user=None, password=None, database=None, collection=None):
    r = redis.Redis(host=host, port=port, db=database)
    return r


@loadenv(db='db_hbase')
def connect_hbase(host=None, port=None, user=None, password=None, database=None, collection=None):
    conn = happybase.Connection(host=host, port=port, timeout=10000)
    return conn




class CK():
    def __init__(self,ck_db=None):
        if ck_db is not None and isinstance(ck_db,dict):
            if  ck_db.__contains__('headers'):
                self.host = ck_db['host']
                self.headers = ck_db['headers']
            else:
                self.dbinfo=ck_db
        else:
            self.dbinfo = None

    def connect_clickhouse(self):

        if self.dbinfo is not None:
            host=self.dbinfo['host']
            port = self.dbinfo['port']
            user = self.dbinfo['user']
            password = self.dbinfo['password']
            database = self.dbinfo['database']
        else:
            env = readconfini('./conf/')
            host=env.get('db_ck','db_host')
            port = env.get('db_ck', 'db_port')
            user = env.get('db_ck', 'db_username')
            password = env.get('db_ck', 'db_password')
            database = env.get('db_ck', 'db_name')
            pass

        conn = connect(host=host, port=port, user=user, password=password, database=database)
        # conn = connect(host=host,user=user, password=password, database=database)
        return conn.cursor()

    # 对于一个大clickhouse sql,通过shell 命令执行ck sql
    def cmd_linux(self, sql):
        '''

        :param sql:
        :return: 返回一个二维列表
        '''
        username_password = self.headers['X-ClickHouse-User'] + ":" + self.headers['X-ClickHouse-Key']
        host=self.host

        url = "http://" + username_password + "@" + host + ":8123"

        cmd = 'curl "' + url + '" -d "' + sql + '"'
        rawresult = os.popen(cmd).readlines()  # 返回一个列表

        result = []
        if len(rawresult) > 0:

            for ele in rawresult:
                result.append(ele.strip('\n').split('\t'))

        return result

    # 一般sql,通过8123端口访问ck
    def ck_get(self,sql):
        url=self.host
        headers=self.headers

        r = requests.get(url=url, params={'query': sql}, headers=headers)
        rawdata = []
        if r.status_code == 200 and len(r.text) > 0:
            rtext = r.text.strip('\n')
            rawdata = [ele.split('\t') for ele in rtext.split('\n')]

        return rawdata

#数据库访问类
class PyDB():

    def __init__(self):
        self.conn = None
        self.cusor = None
        pass

    def close_db(self):
        """关闭数据库"""
        if self.cursor is not None:
            self.cursor.close()
        if self.conn is not None:
            self.conn.close()

    def get_cursor(self):
        return self.cursor

    def get_result_from_db(self,sql):

        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result


class PyCK_client(PyDB):
    '''
    clickhouse访问
    '''

    @loaddbenv(db='db_ck')
    def __init__(self,host=None, port=None, user=None, password=None, database=None, collection=None):
        self.conn = Client(host=host, port=port, user=user, password=password, database=database)
        self.cursor = None
        pass

    def get_result_from_db(self,sql):

        result = self.conn.execute(sql)
        return result
        pass


class PyCK(PyDB):
    '''
    clickhouse 访问
    '''

    @loaddbenv(db='db_ck')
    def __init__(self,host=None, port=None, user=None, password=None, database=None):
        self.conn = connect(host=host, port=port, user=user, password=password, database=database)
        self.cursor = self.conn.cursor()



class PyMysql(PyDB):
    '''
    mysql 访问
    '''
    @loaddbenv(db='db_mysql')
    def __init__(self, host=None, port=None, user=None, password: str = None, database=None,
                 ssh_ip='10.255.254.49', #跳板机地址
                 ssh_username='root', #跳板机用户名
                 ssh_passwd='dell1950', #跳板机密码
                 need_jump = False
                 ):

        if need_jump :
            server = SSHTunnelForwarder(
                ssh_address_or_host=(ssh_ip, 22),
                ssh_username=ssh_username,
                ssh_password=ssh_passwd,
                remote_bind_address=(host, port)
            )
            server.start()
            host = '127.0.0.1'
            port = server.local_bind_port

        self.conn = pymysql.connect(host=host, port=port, user=user, password=password, db=database)
        self.cursor = self.conn.cursor()

class PyMssql(PyDB):
    '''
    sql server 访问
    '''

    @loaddbenv(db='db_ck')
    def __init__(self, host=None, port=None, user=None, password=None, database=None):
        self.conn = pymssql.connect(host=host, port=port, user=user, password=password, database=database)
        self.cursor = self.conn.cursor()


class PyHive(PyDB):
    '''
    hive 访问
    '''
    @loaddbenv(db='db_hive')
    def __init__(self,host=None, port=None, user=None, password=None, database=None):
        # Password should be set if and only if in LDAP or CUSTOM mode; Remove password or use one of those modes
        self.conn = hive.Connection(host=host, port=port, username=user, database=database)
        self.cursor = self.conn.cursor()

