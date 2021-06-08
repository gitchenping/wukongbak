import os
import requests
import json
from clickhouse_driver import Client,connect
import pymssql
# from pyhive import hive
import pymongo
import pymysql
import redis
from .decorate import loadenv
import configparser
from sshtunnel import SSHTunnelForwarder
from utils.load import readconfini


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
    connect = pymssql.connect('10.4.10.184', 'readuser', 'password', db)
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


def close_db(conn, cursor):
    """关闭数据库"""
    cursor.close()
    conn.close()


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


# davinci
def login_davinci():
    user_name = "chenping"
    passwd = "Ddmymm4321"
    s = requests.Session()
    flag, token = do_log(s, user_name, passwd)
    return s, token


def do_log(s, user_name, passwd):
    """
    进行登录
    :param s:
    :return:
    """
    # login_url = "http://10.4.32.223:80/api/v3/login"
    login_url = "http://newwk.dangdang.com/api/v3/login"
    headers = {"Content-Type": "application/json"}
    data = {"username": user_name, "password": passwd}
    r = s.post(url=login_url, json=data, headers=headers)
    req = json.loads(r.content)
    code = req['header']['code']
    token = req['header']['token']
    if code == 200:
        return True, token
    else:
        return False, ''


def readini(path):
    cf = configparser.ConfigParser()
    cf.read(path, encoding='utf-8')
    return cf

#通过跳板机进行远程连接
def connect_mysql_from_jump_server(mysql_ip, db_port, db_user, db_passwd, db,
                                   ip='10.255.254.49',
                                   username='root',
                                   passwd='dell1950'):
    """
    使用跳板机连接远程服务器
    :param ip: 跳板机地址
    :param username:跳板机用户名
    :param passwd: 跳板机密码
    :param mysql_ip: 目标数据库服务器地址
    :param db_user:
    :param db_passwd:
    :param db:
    :return:
    """
    server = SSHTunnelForwarder(
        ssh_address_or_host=(ip, 22),
        ssh_username=username,
        ssh_password=passwd,
        remote_bind_address=(mysql_ip, db_port)
    )
    server.start()
    db = pymysql.connect(
        host='127.0.0.1',
        port=server.local_bind_port,
        user=db_user,
        passwd=db_passwd,
        db=db
    )
    cursor = db.cursor()
    return server, cursor




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
