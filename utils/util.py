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

from sshtunnel import SSHTunnelForwarder




def is_number(s):
    if isinstance(s,str) and s.lower()=='nan':
        return False
    try:
        float(s)
        return True
    except Exception:
        pass

    # try:
    #     import unicodedata
    #     unicodedata.numeric(s)
    #     return True
    # except (TypeError, ValueError):
    #     pass

    return False

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






