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
import datetime
import re
from .map import *


'''数值转换'''
def format_precision(data,selfdefine=0):
    newdata=None
    pattern = re.compile("-?[0-9]+(\.?[0-9]+)?$")         #由数字构成
    try:
        if data is None:
            newdata=0
        elif isinstance(data,float):
            newdata = round(data, 2)
        elif pattern.match(str(data)):
            newdata=round(float(data),2)
        elif data.endswith('%') or data.endswith('万') or data.endswith('元'):                            #如 '-23.56%'
            newdata=float(data.strip('%|万|元'))
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

'''两字典比较，并输出不一样的字典键值对'''
def diff(data1,data2,_logger=None):
    key_diff_dict=diff_dict(data1, data2)
    if key_diff_dict!={}:
        if _logger is not None:
            try:
                _logger.info("diff info:" + str(key_diff_dict)+"-*-Fail-*-")
                # _logger.info("xx"*10+''+"xx"*10)
            except Exception as e:
                _logger.info(e)
            _logger.info('=='*24)


'''两个字典的比较'''
def diff_dict_2(data1,data2,absvalue=0.5):
    # diff_key_value_list=[]
    diff_key_value={}
    if data1=={}:
        temp_data1=dict(data2)
        temp_data2 = {}
    else:
        temp_data1 = dict(data1)
        temp_data2 = dict(data2)
    for key in temp_data1.keys():
        data1_value = temp_data1[key]
        try:
            data2_value=temp_data2[key]
            if isinstance(data1_value,dict):
                # diff_key_value_list.extend(diff(temp_data1[key],temp_data2[key]))
                diff_key_value[key]=diff_dict(data1_value,data2_value)
                if diff_key_value[key]=={}:
                    diff_key_value.pop(key)
            else:
                if isinstance(data1_value,str) and isinstance(data2_value,str):
                    if data1_value!=data2_value:
                        diff_key_value[key] = (data1_value, data2_value)
                else:
                    if abs(data1_value-data2_value) > absvalue:
                        # diff_key_value_list.append({key: (temp_data1[key], temp_data2[key])})
                        diff_key_value[key] = (data1_value, data2_value)
        except KeyError as e:
            key_error_string=key+' 键值对不匹配'
            diff_key_value[key_error_string] = e.__repr__()
        except TypeError as e:
            key_error_string = key + ' 运算类型错误'
            diff_key_value[key_error_string] = (data1_value,data2_value)
        except Exception as e:
            key_error_string = key + ' 其他错误'
            diff_key_value[key_error_string]=e.__repr__()
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
            diff_key_value[key + ' 键不匹配'] = ('',key)
            skip=True
        try:
            data2_value = temp_data2[key]
        except KeyError as e:
            diff_key_value[key + ' 键不匹配'] = ( key,'')
            skip=True
        if skip is False:
            try:
                if isinstance(data1_value, dict) or isinstance(data2_value, dict):
                    # diff_key_value_list.extend(diff(temp_data1[key],temp_data2[key]))
                    diff_key_value[key] = diff_dict(data1_value, data2_value)
                    if diff_key_value[key] == {}:
                        diff_key_value.pop(key)
                else:
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

            # port
            if args.__contains__('port') and args['port'] is not None:
                port = args['port']
            else:
                port = env.get(kwargs['db'], 'db_port')

            #user
            if args.__contains__('user') and args['user'] is not None:
                user = args['user']
            else:
                user = env.get(kwargs['db'], 'db_username')

            #password
            if args.__contains__('password') and args['password'] is not None:
                password = args['password']
            else:
                password = env.get(kwargs['db'], 'db_password')

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
    passwd = "Ddmymm4321"
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

def get_shaixuantiaojian(data=None):

    rtnstr="平台来源："+source_dict[data['source']]+"--"+parent_platform_dict[data['parent_platform']]+"--"+platform_dict[data['platform']]+\
           "&"+"事业部："+bd_id_dict[data['bd_id']]+\
           "&"+"经营方式："+shop_type[data['shop_type']]+\
           "&"+"剔除选项："+eliminate_type_dict[data['eliminate_type']]+\
           "&"+"销售类型："+sale_type_dict[data['sale_type']]
    return rtnstr

def request(url,data=None):
    headers={'Authorization':'Bearer MDAwMDAwMDAwMLGenKE'}
    textdata=requests.get(url,params=data,headers=headers)

    if textdata is not None and textdata.status_code==200:
        return json.loads(textdata.text)

    #请求失败
    return -1


'''传入日期返回对应日周月季的key'''
def get_trendkey(datetype,date):
    '''
    :param datetype:
    :param date: '2020-12-21'
    :return:
    '''

    templist=date.split('-')
    if datetype == 'day' or datetype=='d':

        key = templist[1] + '/' + templist[2]

    elif datetype == 'wtd' or datetype=='w':
        a = datetime.datetime.strptime(date, '%Y-%m-%d')
        key = 'W' + str(a.isocalendar()[1])

    elif datetype == 'mtd' or datetype=='m':
        key = str(int(templist[1])) + '月'

    else:
        key = 'Q' + str(math.ceil(int(templist[1]) / 3))

    return key



