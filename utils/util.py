import os
import requests
import json
import re
import threading


#数据格式转换，便于比较、计算
def data_change(data):
    '''
    格式转换
    :param data:
    :return:
    '''
    temp = dict(data)

    for key,value in temp.items():
        if value is None or value == 'inf':
            temp[key] = '-'
        elif isinstance(value,str):
            try:
               value_new = round(eval(value.strip('%')),2)
            except Exception :
                value_new = value
            temp[key] = value_new
        elif isinstance(value, float):
            temp[key] = round(value,2)
        else:
            continue
    return temp


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


'''两个字典的简单比较（固定一个字典为基准）'''
def simplediff(data1,data2):
    '''

    :param data1: test result,作为参照
    :param data2: dev result
    :return: 不同的键值对
    '''
    diff_key_value={}
    if data1 == {} and data2 != {}:
        diff_key_value['KeyError'] ={'test':{},'dev':data2}
    else:
        for key in data1.keys():
            data1_value = data1[key]
            try:
                data2_value = data2[key]
            except KeyError:
                diff_key_value[key] = 'dev table 中此键值不存在'
                continue
            if isinstance(data1_value,dict):
                diff_key_value[key] = simplediff(data1_value,data2_value)
                if diff_key_value[key] == {}:
                    diff_key_value.pop(key)
            else:
                if data1_value != data2_value:
                    diff_key_value[key] = {'test':data1_value, 'dev':data2_value}
    return diff_key_value

'''两个字典比较'''
def diff_dict(data1, data2, absvalue = 0.5):
    '''

    :param data1: test result
    :param data2: dev result
    :param absvalue: 允许误差
    :return:
    '''
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
                            diff_key_value[key] = {'test':data1_value, 'dev':data2_value}
                    else:
                        if abs(data1_value - data2_value) > absvalue:
                            # diff_key_value_list.append({key: (temp_data1[key], temp_data2[key])})
                            diff_key_value[key] = {'test':data1_value, 'dev':data2_value}
            except TypeError as e:
                diff_key_value[key + ' 运算类型错误'] = {'test':data1_value, 'dev':data2_value}
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

'''找到一个列表中，有指定相同字段的子元素'''
def  find_same_id_list(i,data,totalnum=1):
    '''

    :param i: 开始寻找位置
    :param data: 列表,已按id排序好
    :param totalnum: 相同字段元素个数限制
    :return:
    '''

    length=len(data)
    result = {}

    if i < length and i >= 0:

        ele = data[i]
        id = ele[0]

        #剩余字段如何组织
        result[id] = {ele[1]: ele[2]}

        loop_down = 1
        # 向下找相同的custid
        while loop_down <= totalnum and i + loop_down <= length - 1:
            nextele = data[i + loop_down]

            if nextele[0] != id:
                break
            result[id].update({nextele[1]: nextele[2]})
            loop_down += 1

        # 向上找3 - loop_down 个
        loop_up = 1
        while loop_up <= totalnum - loop_down and i - loop_up >= 0:
            forwardele = data[i - loop_up]

            if forwardele[0] != id:
                break

            result[id].update({forwardele[1]: int(forwardele[2])})
            loop_up += 1

    return result

'''自定义线程类，实例化时传入一个函数及其参数'''
class myThread(threading.Thread):
    def __init__(self,func,*arg,**kwargs):
        '''

        :param func: task 函数
        :param arg:  task 包裹位置参数
        :param kwargs: task 包裹关键字参数
        '''
        threading.Thread.__init__(self)
        self.arg = arg
        self.kw = kwargs
        self.func = func

    def run(self):
        self.result= self.func(*self.arg,**self.kw)

    #返回值
    def get_result(self):
        try:
            return self.result
        except Exception:
            return None




