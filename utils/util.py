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

def diff_sd(data1,data2,_logger):
    # 字典比较
    temp_data1 = dict(data1)
    temp_data2 = dict(data2)

    info = ''
    key_value_info = ''
    diff_key_values = []
    if set(temp_data1.keys()) != set(temp_data2.keys()):
        info = "键值不同"
    else:
        for item in temp_data1.keys():
            for key in temp_data1[item].keys():
                try:
                    if temp_data1[item][key] != temp_data2[item][key]:
                            diff_key_values.append({item + "->" + key: (temp_data1[item][key], temp_data2[item][key])})
                except Exception as e:
                    info = '出现异常，...'
                    diff_key_values.append(repr(e))
        if len(diff_key_values) > 0:
            info = "键值对不同"
            key_value_info = str(diff_key_values)

    if len(info) > 0:
        try:
            _logger.info("diff info:" + info)
            if key_value_info != '':
                _logger.info(key_value_info)
            _logger.info(
                "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-Fail-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        except Exception as e:
            _logger.info(e)
    _logger.info(
        '==========================================================================================================')


'''两重嵌套字典比较'''
def diff_d(data1,data2,_logger):
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
        for item in temp_data1.keys():
            for key in temp_data1[item].keys():
                try :
                    if temp_data1[item][key] != temp_data2[item][key]:
                        if not isinstance(temp_data1[item][key],str) and not isinstance(temp_data2[item][key],str):
                            if abs(temp_data1[item][key]-temp_data2[item][key])>0.5:
                                diff_key_values.append({item+"->"+key: (temp_data1[item][key], temp_data2[item][key])})
                        else:
                            diff_key_values.append({item+"->"+key: (temp_data1[item][key], temp_data2[item][key])})
                except Exception as e:
                    info='出现异常，...'
                    diff_key_values.append(repr(e))
        if len(diff_key_values) > 0:
            info = "键值对不同"
            key_value_info=str(diff_key_values)

    if len(info)>0:
        try:
            _logger.info("diff info:"+ info)
            if key_value_info !='':
                _logger.info(key_value_info)
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

def get_date_in_w_m_q(datetype, date):
    templist=date.split('-')
    year=templist[0]
    m=int(templist[1])

    if datetype == 'day':
        key = date
    elif datetype == 'wtd':
        _date = datetime.datetime.strptime(date, '%Y-%m-%d')
        a=_date.weekday()

        key=datetime.datetime.strftime(_date - datetime.timedelta(days=a),'%Y-%m-%d')
    elif datetype=='mtd':
        key=year+"-"+templist[1]+"-"+"01"

    else:
        key=str(year)+get_day2q(m)[0]
    return key

def datechange(type,enddate):
    '''返回日期所在的日、周、月、季'''
    templist = enddate.split('-')
    date_type=type
    # date转化
    key=''
    if date_type == 'day':
        key = enddate
    elif date_type == 'wtd':
        a = datetime.datetime.strptime(enddate, '%Y-%m-%d')
        key = templist[0] + '-w' + str(a.isocalendar()[1])
    elif date_type == 'mtd':
        key = templist[0] + '-m' + str(int(templist[1]))
    else:
        key = templist[0] + '-q' + str(math.ceil(int(templist[1]) / 3))
    return key

def get_day2month(year,month):
    '''输出日期所在的月初和月末'''
    if month=='2':
        # 闰年 2月份有29天
        a=int(year)
        if  a % 400==0 or (a % 4==0 and a % 100!=0):
            lastday='29'
        else:
            lastday='28'
    elif month in ['4','6','9','11'] :
        lastday='30'
    else:
        lastday='31'

    return str(year)+"-"+str(month)+"-"+"01",str(year)+"-"+str(month)+"-"+lastday

def get_day2q(m):

    tmp=math.ceil(m / 3)
    rtn=''
    if tmp == 1:
        rtn=("-01-01","-03-31")
    if tmp == 2:
        rtn = ("-04-01", "-06-30")
    if tmp == 3:
        rtn = ("-07-01", "-09-30")
    if tmp == 4:
        rtn = ("-10-01", "-12-31")
    return rtn

def get_tb_hb_date(date,datetype):
    tb_hb_date=None

    datelist=date.split('-')

    year=int(datelist[0])
    month=int(datelist[1])
    day=int(datelist[2])

    _date = datetime.datetime.strptime(date, '%Y-%m-%d')

    if datetype == 'day' or datetype == 'd':

        #今天

        # 昨天
        day_one= datetime.datetime.strftime(_date - datetime.timedelta(days=1),'%Y-%m-%d')
        # 上周的今天
        day_two = datetime.datetime.strftime(_date - datetime.timedelta(days=7),'%Y-%m-%d')

        # 去年今天
        tmp=int(datelist[0])-1
        datelist[0]=str(tmp)
        day_three='-'.join(datelist)
        tb_hb_date=(date,day_one,day_three,day_two)
    elif datetype=='wtd' or datetype == 'w':

        #本周
        a=_date.weekday()
        week_s = datetime.datetime.strftime(_date - datetime.timedelta(days=a), '%Y-%m-%d')
        week_e=date

        #环比上周
        last_week_e=datetime.datetime.strftime(_date - datetime.timedelta(days=7),'%Y-%m-%d')
        last_week_s = datetime.datetime.strftime(_date - datetime.timedelta(days=a)- datetime.timedelta(days=7), '%Y-%m-%d')

        #去年的这个周
        week_num=_date.isocalendar()[1]
        year-=1
        a = time.strptime(str(year)+"-"+str(week_num)+'-0', '%Y-%U-%w')
        last_year_week_e=str(a.tm_year)+"-"+str(a.tm_mon)+"-"+str(a.tm_mday)
        last_year_week_s = datetime.datetime.strftime(datetime.datetime.strptime(last_year_week_e, '%Y-%m-%d')-datetime.timedelta(days=6), '%Y-%m-%d')
        tb_hb_date = ((week_s,week_e),(last_week_s,last_week_e), (last_year_week_s,last_year_week_e))

    elif datetype=='mtd' or datetype == 'm':

        #本月
        m=get_day2month(year,month)

        # 环比上月
        month -=1
        hb_month=get_day2month(str(year),str(month))

        # 同比去年
        month = month+1
        last_year = year - 1
        tb_month=get_day2month(str(last_year),str(month))
        tb_hb_date=(m,hb_month,tb_month)

    else:
        #本季度
        q_=get_day2q(month)
        q=(str(year)+q_[0],str(year)+q_[1])

        # 环比上季度
        m=month-3
        tmp=get_day2q(m)
        hb_q=(str(year)+tmp[0],str(year)+tmp[1])
        # 同比去年
        m = month
        y = str(year - 1)
        tmp = get_day2q(m)
        tb_q = (y + tmp[0], y + tmp[1])
        tb_hb_date =(q,hb_q,tb_q)
    return tb_hb_date


def get_trend_where_date(data):
    datetype=data['date_type']
    datestr=data['date_str']

    datelist=[]
    wheredate=''
    wheredata=''

    templist=datestr.split('-')
    if datetype == 'day' or datetype == 'd':       #最近七天
        i=0
        end_date_datetime = datetime.datetime.strptime(datestr, '%Y-%m-%d')
        while i<7:
            datelist.append(end_date_datetime.strftime("%Y-%m-%d"))
            delta = datetime.timedelta(days=1)
            end_date_datetime-= delta

            i+=1

        for date in datelist:
            wheredate+="'"+date+"',"
        wheredate=wheredate.strip(',')
        wheredata=' and date_str in ('+wheredate+")"

    if datetype == 'wtd' or datetype == 'w':  # 最近七天

        end_date_datetime = datetime.datetime.strptime(datestr, '%Y-%m-%d')
        i=0
        while i < 7:
            tempdatetime_e=end_date_datetime-datetime.timedelta(days=7 * i)
            tempdatetime_s=tempdatetime_e-datetime.timedelta(days=6)

            wheredate += " date_str between '" +tempdatetime_s.strftime("%Y-%m-%d") + "' and '" + tempdatetime_e.strftime("%Y-%m-%d") + "' or "

            i+=1

        wheredata = ' and (' + wheredate.strip('or ')+")"

        pass

    if datetype == 'mtd' or datetype == 'm':  # 最近七个月
        year=templist[0]
        month=templist[1]
        i=0
        while i<7:
            month_s_e=get_day2month(year,month)

            wheredate += " date_str between '" + month_s_e[0] + "' and '" +month_s_e[1] + "' or "

            month=str(int(month)-1)
            i+=1

        wheredata = ' and ('+ wheredate.strip('or ')+")"

        pass

    if datetype == 'qtd' or datetype == 'q':  # 最近3个季度
        year = templist[0]
        month = templist[1]

        i=0
        while i< int(month)/3:
            a=int(month)-3*i
            q_s_e =get_day2q(a)

            wheredate += " date_str between '" +year+ q_s_e[0] + "' and '" +year+ q_s_e[1] + "' or "
            i+=1

        wheredata = ' and (' + wheredate.strip('or ')+")"

    return wheredata

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