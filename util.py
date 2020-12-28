import os
import math
import requests
import json
from clickhouse_driver import Client,connect
import pymssql
# from pyhive import hive
import configparser
import pymongo
import pymysql
import redis


'''数值转换'''
def format_precision(data):
    if type(data)!=str and math.isnan(data):       #无穷大
        date=0
    elif isinstance(data,float):                   #浮点型
        date=round(data,2)
    elif type(data)==str and data.isdigit():      #整数型字符串
        date=int(data)
    return date

'''单字典比较'''
def diff_s(data1,data2,_logger):
    '''
    :param data1:
    :param data2:
    :param _log:  自定义logger
    :return:
    '''
    # 字典比较
    temp_data1 = dict(data1)
    temp_data2 = dict(data2)

    info=''
    key_value_info=''
    diff_key_values = []
    if set(temp_data1.keys())!=set(temp_data2.keys()):
        info="键值不同"
    else:
        for key in temp_data1.keys():
            if data1[key] != data2[key]:
                diff_key_values.append({key: (temp_data1[key], temp_data2[key])})
        if len(diff_key_values) > 0:
            info = "键值对不同"
            key_value_info=str(diff_key_values)

    if len(info)>0:
        try:
            _logger.info("diff info:"+ info)
            if key_value_info !='':
                _logger.info(str([temp_data1,temp_data2]))
            _logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-Fail-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        except Exception as e:
            _logger.info(e)
    _logger.info('==========================================================================================================')

'''输出两个字典的不同'''
def diff(data1,data2):
    '''
    :param data1:
    :param data2:
    :param _log:  自定义logger
    :return:
    '''
    # 字典比较
    temp_data1 = dict(data1)
    temp_data2 = dict(data2)

    info=''
    key_value_info=''
    diff_key_values = []
    if set(temp_data1.keys())!=set(temp_data2.keys()):
        info="键值不同"
    else:
        for key in temp_data1.keys():
            if data1[key] != data2[key]:
                diff_key_values.append({key: (temp_data1[key], temp_data2[key])})
        if len(diff_key_values) > 0:
            info = "键值对不同"
            key_value_info=str(diff_key_values)
    return info,key_value_info

'''重试'''
def retry(maxretry=3):
    def decorator(func):
        def wrapper(*args, **kw):
            att = 0
            while att < maxretry:
                try:
                    return func(*args, **kw)
                except Exception as e:
                    att += 1
        return wrapper
    return decorator

'''数据库连接'''

def readconfini(path=''):
    cf = configparser.ConfigParser()
    allpath=os.path.join(path, "config.ini")
    cf.read(allpath, encoding='utf-8')

    return cf

def loadenv(**kwargs):
    env = readconfini('./')
    # host=env.get(kwargs['db'],'db_host')
    port=env.get(kwargs['db'],'db_port')
    user=env.get(kwargs['db'],'db_username')
    password=env.get(kwargs['db'],'db_password')

    def wrap_o(func):
        def wrap(**args):       #如果函数带参数，使用实参

            #host
            if args.__contains__('host') and args['host'] is not None:
                host=args['host']
            else:
                host = env.get(kwargs['db'], 'db_host')

            #database
            if args.__contains__('database') and args['database'] is not None:
                database=args['database']
            else:
                database = env.get(kwargs['db'], 'db_name')

            #collection
            if args.__contains__('collection') and args['collection'] is not None:
                collection=args['collection']
            else:
                if env.has_option(kwargs['db'],'db_collection'):
                    collection = env.get(kwargs['db'],'db_collection')
                else:
                    collection = None
            return func(host,port,user,password,database,collection)
        return wrap
    return wrap_o
    pass

@loadenv(db='db_ck')
def connect_clickhouse(host=None,port=None, user=None, password=None, database=None,collection=None):
    # conn = connect(host=host,port=port,user=user,password=password, database=database)
    conn = connect(host=host,user=user, password=password, database=database)
    return conn.cursor()

@loadenv(db='db_ck')
def client_ck(host=None,port=None, user=None, password=None, database=None,collection=None):
    """链接ck数据库"""
    conn = Client(host=host, user=user,password=password, database=database)
    return conn


def connect_sqlserver(db):
    """连接sql server数据库"""
    connect = pymssql.connect('10.4.10.184', 'readuser', 'password', db)
    cursor = connect.cursor()
    return connect, cursor

def request(url,data=None):
    headers={'Authorization':'Bearer MDAwMDAwMDAwMLGenKE'}
    textdata=requests.get(url,params=data,headers=headers)

    if textdata is not None and textdata.status_code==200:
        return json.loads(textdata.text)

    #请求失败
    return -1


@loadenv(db='db_hive')
def connect_hive(host=None,port=None, user=None, password=None, database=None,collection=None):
    #Password should be set if and only if in LDAP or CUSTOM mode; Remove password or use one of those modes
    conn = hive.Connection(host=host,port=port, username=user, database=database)
    return conn.cursor()

@loadenv(db='db_mysql')
def connect_mysql(host=None,port=None, user=None, password=None, database=None,collection=None):

    conn=pymysql.connect(host=host,user=user,password=password,db=database)
    return conn.cursor()


def close_db(conn, cursor):
    """关闭数据库"""
    cursor.close()
    conn.close()

def readini(path):
    cf=configparser.ConfigParser()
    cf.read(path,encoding='utf-8')
    return cf

#连接mongodb
@loadenv(db='db_mongo')
def connect_mongodb(host=None,port=None, user=None, password=None, database=None,collection=None):
    """链接mongodb"""
    client = pymongo.MongoClient("mongodb://"+host+":"+port+"/")
    client.admin.authenticate(user,password, mechanism='SCRAM-SHA-1')

    db = client[database]
    coll = db[collection]
    return coll

@loadenv(db='db_redis')
def get_redis(host=None,port=None, user=None, password=None, database=None,collection=None):
    r = redis.Redis(host=host, port=port, db=database)
    return r

#davinci
def login_davinci():
    user_name = "chenping"
    passwd = "Ddmymm1234"
    s = requests.Session()
    flag, token = do_log(s, user_name, passwd)
    # headers = {"Content-Type": "application/json", "Authorization":"Bearer " + token}
    return s,token
def do_log(s, user_name, passwd):
    """
    进行登录
    :param s:
    :return:
    """
    # login_url = "http://10.4.32.223:80/api/v3/login"
    login_url="http://newwk.dangdang.com/api/v3/login"
    headers = {"Content-Type": "application/json"}
    data = {"username":user_name, "password":passwd}
    r = s.post(url=login_url, json=data, headers=headers)
    req = json.loads(r.content)
    code = req['header']['code']
    token = req['header']['token']
    if code == 200:
        return True, token
    else:
        return False, ''
