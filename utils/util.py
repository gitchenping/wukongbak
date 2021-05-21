import os
import requests
import json
from clickhouse_driver import Client,connect
import pymssql
# from pyhive import hive
import pymongo
import pymysql
import redis
import re
from .decorate import loadenv
import configparser
from sshtunnel import SSHTunnelForwarder

'''数值(精度)转换'''
def format_precision(data,selfdefine=0):
    '''

    :param data: str、int、float
    :param selfdefine:
    :return:
    '''
    newdata=None

    if data is None:
        return 0
    else:
        tempdata=str(data).rstrip('%|亿|万|元')
        pattern = re.compile("-?[0-9]+(\.?[0-9]+)?$")         #由数字构成
        try:
            if pattern.match(tempdata):
                newdata=round(float(tempdata),2)
            else:
                if selfdefine=='':
                    newdata=data
                else:
                    newdata=selfdefine
        except Exception:
            newdata=data
    return newdata


'''字典数据类型数值格式化'''
def json_format(data,selfdefine):
    '''

    :param data: 嵌套字典
    :return:
    '''
    newdata={}
    if not isinstance(data,dict):
       return format_precision(data,selfdefine)
    for key in data.keys():
            value = data[key]
            if isinstance(value, dict):
                newdata[key] = json_format(value,selfdefine)
            else:
                #
                newdata[key]=format_precision(value,selfdefine)
    return newdata


'''两个字典的简单比较（固定一个字典）'''
def simplediff(data1,data2):
    '''

    :param data1:
    :param data2:
    :return: 不同的键值对
    '''
    diff_key_value={}
    for key in data1.keys():
        data1_value = data1[key]
        try:
            data2_value = data2[key]
        except KeyError:
            diff_key_value[key] = 'mysql 中此键值不存在'
            continue
        if isinstance(data1_value,dict):
            diff_key_value[key]=simplediff(data1_value,data2_value)
            if diff_key_value[key]=={}:
                diff_key_value.pop(key)
        else:
            if data1_value!=data2_value:
                diff_key_value[key] = (data1_value, data2_value)
    return diff_key_value

'''两个字典比较'''
def diff_dict(data1, data2, absvalue=0.5):
    diff_key_value = {}
    temp_data1 = dict(data1)
    temp_data2 = dict(data2)
    set_data1 = set(temp_data1.keys())
    set_data2 = set(temp_data2.keys())
    set_all = set_data1.union(set_data2)

    for key in set_all:
        skip = False                          #是否需要比较,键值对不匹配的跳过比较逻辑
        try:
            data1_value = temp_data1[key]
        except KeyError as e:
            diff_key_value[key + ' 键不匹配'] = ('',key)    #data1没有此键
            skip=True
        try:
            data2_value = temp_data2[key]
        except KeyError as e:
            diff_key_value[key + ' 键不匹配'] = ( key,'')   #data2没有此键
            skip=True
        if skip is False:
            try:
                if isinstance(data1_value, dict) or isinstance(data2_value, dict):
                    # diff_key_value_list.extend(diff(temp_data1[key],temp_data2[key]))
                    diff_key_value[key] = diff_dict(data1_value, data2_value)
                    if diff_key_value[key] == {}:
                        diff_key_value.pop(key)
                else:
                    if data1_value is None and data2_value is None:
                        continue
                    if isinstance(data1_value, str) and isinstance(data2_value, str):
                        if data1_value != data2_value:
                            diff_key_value[key] = (data1_value, data2_value)
                    else:
                        if abs(data1_value - data2_value) > absvalue:
                            # diff_key_value_list.append({key: (temp_data1[key], temp_data2[key])})
                            diff_key_value[key] = (data1_value, data2_value)
            except TypeError as e:
                diff_key_value[key + ' 运算类型错误'] = (data1_value, data2_value)
            except Exception as e:
                diff_key_value[key + ' 其他错误'] = e.__repr__()
    return diff_key_value

'''数据库连接'''
@loadenv(db='db_ck')
def connect_clickhouse(host=None,port=None, user=None, password=None, database=None,collection=None):
    conn = connect(host=host,port=port,user=user,password=password, database=database)
    # conn = connect(host=host,user=user, password=password, database=database)
    return conn.cursor()

@loadenv(db='db_ck')
def client_ck(host=None,port=None, user=None, password=None, database=None,collection=None):
    """链接ck数据库"""
    conn = Client(host=host,port=port,user=user,password=password, database=database)
    return conn


def connect_sqlserver(db):
    """连接sql server数据库"""
    connect = pymssql.connect('10.4.10.184', 'readuser', 'password', db)
    cursor = connect.cursor()
    return connect, cursor


@loadenv(db='db_hive')
def connect_hive(host=None,port=None, user=None, password=None, database=None,collection=None):
    #Password should be set if and only if in LDAP or CUSTOM mode; Remove password or use one of those modes
    conn = hive.Connection(host=host,port=port, username=user, database=database)
    return conn.cursor()

@loadenv(db='db_mysql')
def connect_mysql(host=None,port=None, user=None, password=None, database=None,collection=None):
    conn=pymysql.connect(host=host,port=port,user=user,password=password,db=database)
    return conn.cursor()


def close_db(conn, cursor):
    """关闭数据库"""
    cursor.close()
    conn.close()



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
    passwd = "Ddmymm4321"
    s = requests.Session()
    flag, token = do_log(s, user_name, passwd)
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



def readini(path):
    cf=configparser.ConfigParser()
    cf.read(path,encoding='utf-8')
    return cf

def connect_mysql_from_jump_server(mysql_ip, db_port,db_user, db_passwd, db,
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


#通过shell 命令执行ck sql
def cmd_linux(sql):

    cmd='curl "http://membersbi:dangdangbi@10.0.5.80:8123" -d "'+sql+'"'
    rawresult=os.popen(cmd).readlines()     #返回一个列表

    result=[]
    if len(rawresult)>0:

        for ele in rawresult:
            result.append(ele.strip('\n').split('\t'))

    return result