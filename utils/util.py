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
import time
from .variable import *


'''数值转换'''
def format_precision(data):
    if type(data)!=str and math.isnan(data):       #无穷大
        date=0
    elif isinstance(data,float):                   #浮点型
        date=round(data,2)
    elif type(data)==str and data.isdigit():      #整数型字符串
        date=int(data)
    return date

def format_trans(data):
    if data !='--':
        return round(float(data),2)
    else:
        return data

def tb_hb_format(data):
    if isinstance(data,str) and data.endswith('%'):
        return float(data.strip('%'))
    else:
        return data



def diff(data1,data2,_logger):
    key_diff_dict=diff_dict(data1, data2)
    if key_diff_dict!={}:
        try:
            _logger.info("diff info:" + str(key_diff_dict))
            _logger.info(
                "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-Fail-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        except Exception as e:
            _logger.info(e)
    _logger.info(
        '==========================================================================================================')


'''两个字典的比较'''
def diff_dict(data1,data2,absvalue=0.5):
    temp_data1 = dict(data1)
    temp_data2 = dict(data2)
    # diff_key_value_list=[]
    diff_key_value={}
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
            key_error_string=key+' 键值对不存在'
            diff_key_value[key_error_string] = e.__repr__()
        except TypeError as e:
            key_error_string = key + ' 运算类型错误'
            diff_key_value[key_error_string] = e.__repr__()
        except Exception as e:
            key_error_string = key + ' 其他错误'
            diff_key_value[key_error_string]=e.__repr__()
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


def get_day_mtd_qtd(date_type,start_date,end_date):
    '''输入起始日期，返回在此范围内的所有日期'''

    datelist=[]
    start_date_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    if date_type == "day":
        # 计算有多少天
        delta = datetime.timedelta(days=1)
        while start_date_datetime<=end_date_datetime:
                datelist.append(start_date_datetime.strftime("%Y-%m-%d"))
                start_date_datetime+=delta

    elif date_type== "mtd":
        # 计算有多
        while start_date_datetime<=end_date_datetime:
            datelist.append(start_date_datetime.strftime("%Y-M%m"))
            start_date_datetime=start_date_datetime.replace(month=start_date_datetime.month + 1)
        # delta=relativedelta(months=1)

        pass
    else:
        # 计算有多少季
        st=start_date.split('-')
        et=end_date.split('-')

        sq=math.ceil(int(st[1]) / 3)
        eq= math.ceil(int(et[1]) /3)
        while sq<=eq:
            datelist.append(st[0]+"-Q"+str(sq))
            sq+=1

        pass
    return datelist

def get_shaixuantiaojian(data=None):

    rtnstr="平台来源："+source[data['source']]+"--"+parent_platform[data['parent_platform']]+"--"+platform[data['platform']]+\
           "&"+"事业部："+bd_id[data['bd_id']]+\
           "&"+"经营方式："+shop_type[data['shop_type']]+\
           "&"+"剔除选项："+eliminate_type[data['eliminate_type']]+\
           "&"+"销售类型："+sale_type[data['sale_type']]
    return rtnstr

def request(url,data=None):
    headers={'Authorization':'Bearer MDAwMDAwMDAwMLGenKE'}
    textdata=requests.get(url,params=data,headers=headers)

    if textdata is not None and textdata.status_code==200:
        return json.loads(textdata.text)

    #请求失败
    return -1






def get_trendkey(datetype,date):

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



def tb_hb_cal(rawdata):
    '''同比、环比计算，返回一个二维列表'''
    newdata=[]
    if len(rawdata)==1:
        return  [[]]
    currentvaluelist = list(rawdata[0])
    newdata.append(currentvaluelist)
    for raw in rawdata[1:]:
        tempdata = []
        for i in range(len(raw) - 1):
            if raw[i] is not None and raw[i] != 0:
                tempdata.append(round((currentvaluelist[i] - raw[i]) / raw[i] * 100, 2))
            else:
                tempdata.append('--')
        tempdata.append(raw[-1])
        newdata.append(tempdata)
    newdata[0] = [round(ele, 2) for ele in currentvaluelist[:-1] if ele is not None]+[currentvaluelist[-1]]

    return newdata