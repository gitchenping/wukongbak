#!/usr/bin/env python
#!encoding=utf8
#
import os
import sys
import platform

sys.path.append("..")
cwd = os.getcwd()
parent = os.path.dirname(cwd)
father_path = os.path.abspath(parent + os.path.sep + "..")
sys.path.append(father_path)
from itertools import combinations
import time
from datetime import datetime, timedelta,date
import calendar
from clickhouse_driver import Client
import re
import math
import calendar

def get_platform():
    """获取系统信息"""
    return platform.architecture()[1]


platform = get_platform()
the_local = ""
conf_file = ""
if platform.startswith("WindowsPE"):
    the_local = "../"
else:
    the_local = "../bi/wk_mobile_offline_newdatasource"


def ck_connect():
    conn = Client(host='10.12.6.235', user='membersbi',
                  password='dangdangbi', database='bi_mdata')
    return conn


def datetime_fmt(date_str):
    # 传入的data_date是字符串，需要转换成时间格式
    start_time = datetime.strptime(date_str, '%Y-%m-%d')
    # print(start_time)
    return start_time

def get_end_time(start_time, days):
    '''
    返回距离现在时间几天的结束时间
    :param start_time:
    :param days: 传入正数，就是比现在日期大的是假；传入负数，就是计算比现在时间早期的时间
    :return:
    '''
    start_time = datetime_fmt(start_time)
    end = start_time + timedelta(days=days)
    # 把datetime类型转换成str返回
    end_time = end.strftime('%Y-%m-%d')
    return end_time


def get_sql_week(date):
    now_date = datetime_fmt(date)
    # datetime.weekday 返回当前日期是本周的第几天  now - timedelta(days=now.weekday()
    n = datetime.weekday(now_date)
    weeklist = []
    for i in range(7):
        this_day = now_date + timedelta(0 - n + i)
        weeklist.append(this_day)
    week_start = weeklist[0].strftime('%Y-%m-%d')
    week_end = weeklist[6].strftime('%Y-%m-%d')
    if datetime_fmt(week_end) >= now_date:
        return week_start, now_date.strftime('%Y-%m-%d')
    else:
        return week_start, week_end


def get_sql_month(date_str):
    date_time = datetime_fmt(date_str)
    month_start_time = date_time.replace(day=1).strftime('%Y-%m-%d')
    _, days_in_month = calendar.monthrange(date_time.year, date_time.month)
    month_end = datetime_fmt(month_start_time) + timedelta(days=days_in_month - 1)
    month_end_time = month_end.strftime('%Y-%m-%d')
    if datetime_fmt(month_end_time) <= date_time:
        return month_start_time, month_end_time
    else:
        return month_start_time, date_time.strftime('%Y-%m-%d')


def get_sql_quarter(date_str):
    """
    移动悟空实时模块计算开始结束时间
    :param date_str:
    :return:
    """
    date_time = datetime_fmt(date_str)
    # 获取当前时间所在月份
    m = date_time.month
    y = date_time.year
    q_month = m-1 - (m - 1) % 3 + 1
    start_time = datetime(y, q_month, 1).strftime('%Y-%m-%d')
    if q_month < 10:
        q_end_time = datetime(y, q_month + 3, 1)
    else:
        q_end_time = datetime(y, 12, 31)
    if q_end_time <= date_time:
            end_time = q_end_time.strftime('%Y-%m-%d')
    else:
        end_time = date_time.strftime('%Y-%m-%d')
    return start_time, end_time


def get_week_order(date):
    """
    返回移动悟空接口需要的周参数W45
    """
    now_date = datetime_fmt(date)
    # 计算当前日期在本年度的第多少周
    year = str(now_date.year)
    now_week = now_date.isocalendar()[1]
    w_order = year + '-' + 'W' + str(now_week)
    return w_order


def get_month_order(date):
    now = datetime_fmt(date)
    year = str(now.year)
    month = now.month
    m_str = year + '-' + 'M' + str(month)
    return m_str


def get_quarter_order(date):
    """
    根据当前月份，返回月份所在季度例如Q1
    :param date:
    :return:
    """
    now = datetime_fmt(date)
    year = str(now.year)
    month = now.month
    m_str = str(month)
    if m_str in ('01', '02', '03'):
        return year + '-' + 'Q1'
    elif m_str in ('04', '05', '06'):
        return year + '-' + 'Q2'
    elif m_str in ('07', '08', '09'):
        return year + '-' + 'Q3'
    elif m_str in ('10', '11', '12'):
        return year + '-' + 'Q4'


def get_last_year_date(date_str):
    """ 获取去年的同一天，计算同比"""
    date = datetime_fmt(date_str)
    year = date.year
    last_year = year-1
    last_year_date = str(last_year) + "-" + str(date.month) + "-" + str(date.day)
    return last_year_date

def get_city_info():
    city_list = []
    with open('mdata_city_dict.txt','r+',encoding='UTF-8') as f:
        lines = f.readlines()
        #line ='"city_id"\t"city_name"\n'
        for line in lines[1:]:
            line = line.strip('\n')
            city_id = re.sub('\D','',line.split('\t')[0])
            city_name = re.findall('[\u4e00-\u9fa5]+', line.split('\t')[1])
            city_map = {city_id: city_name[0]}
            city_list.append(city_map)
        return city_list


def get_city_name(id):
    city_list = get_city_info()
    for i in range(len(city_list)):
        if id in city_list[i].keys():
            return  city_list[i][id]
        else:
            i += 1


def get_customer_format(customer_flag):
    if customer_flag == 1:
        return "新客"
    elif customer_flag == 2:
        return "老客"


def format_amount(amount):
    """处理金额显示格式，把float转成成xxx.x万格式"""
    if amount is None:
        return '--'
    elif math.isnan(amount):
        return '--'
    else:
        if abs(amount) > 100000000:
            tmp=round(float(amount) / 100000000, 2)
            tmp_2 = '%.2f' % tmp
            format_amt = tmp_2 + ''.join('亿')
            return format_amt
        elif 10000 < abs(amount) < 100000000:
            tmp=round(float(amount) / 10000, 2)
            tmp_2='%.2f' % tmp
            format_amt=tmp_2 + ''.join('万')
            return format_amt
        elif amount == 0.0 or amount == 0:
            return '--'
        else:
            if type(amount) is int:
                return str(amount) + '元'
            else:
                return '%.2f' % round(amount, 2) + '元'

def format_person_num(cust_num):
    """查询到的用户总数进行格式处理，方便与api进行比较"""
    if cust_num is None:
        return '--'
    elif math.isnan(cust_num):
        return '--'
    else:
        if abs(cust_num) > 100000000:
            tmp=round(float(cust_num) / 100000000, 2)
            tmp_2 = '%.2f' % tmp
            format_amt = tmp_2 + ''.join('亿')
            return format_amt
        elif 10000 < abs(cust_num) < 100000000:
            tmp=round(float(cust_num) / 10000, 2)
            tmp_2='%.2f' % tmp
            format_amt=tmp_2 + ''.join('万')
            return format_amt
        elif cust_num == 0.0 or cust_num == 0:
            return '--'
        else:
            if type(cust_num) is int:
                return str(cust_num)
            else:
                return '%.2f' % round(cust_num, 2)

def format_ratio(ratio):
    """ 给sql中计算出的占比结尾加上% """
    if ratio is None:
        return '--'
    elif math.isnan(ratio):
        return '--'
    else:
        ratio_str = '%.2f' % ratio + '%'
        return ratio_str


def format_date_by_day(date_str):
    """ 返回month/day格式的时间日期，提供给trend下钻页使用"""
    # print(date_str) #2020-12-24
    month = date_str.month
    day = date_str.day
    if month < 10:
        month_str = '0' + str(month)
    else :
        month_str = str(month)
    if day < 10:
        day_str = '0' + str(day)
    else:
        day_str = str(day)
    return month_str + "/" + day_str



def get_weekdays_by_num(date_str,m):
    """ 根据当前给定的日期，获取前m周的每周开始和结束日期,因为要获取当周时间，所以需要传入周日对应的时间
    """
    date = datetime_fmt(date_str)
    pacing = date.isoweekday() # 或者当前日期是星期几5
    weeks_list = []
    for i in range(0, m):
        last_week_end = get_end_time(date_str,-7*i)
        if pacing == 7:
            last_week_start = get_end_time(last_week_end,-(pacing-1))
        else:
            last_week_start=get_end_time(last_week_end, -(pacing-1))
        weeks_list.append((last_week_start,last_week_end))
    return weeks_list


def get_monthdays_by_num(date_str,m):
    """
    根据当前时间，返回该月季前几个月，每个月的开始和结束日期，用于下钻页进行月的环比计算使用,如果当前月不是一整月，那么环比的月份，也是取
    和当前月一样的天数
    :param date_str: 当前月份日期
    :param m: 返回几个月
    :return: [('2020-12-01', '2020-12-31'), ('2020-11-01', '2020-11-30')]
    """
    date = datetime_fmt(date_str)
    day = date.day
    year = date.year
    month = date.month
    months_list = []
    now_year = datetime.now().year
    if year == now_year:
        for i in range(0, m):
            last_month = month-i
            wweek,end = calendar.monthrange(year,last_month)
            # 如果返回月份的结束日期小于当前的day，要取该月份实际的day，如12月31日，返回11月的结束日期就是11月30
            first_day="%s-%s-01" %(year,last_month)
            if end > day:
                last_day="%s-%s-%s" % (year,last_month,day)
            else:
                last_day="%s-%s-%s" % (year, last_month, end)
            months_list.append((first_day,last_day))
    else:
        for i in range(0, m):
            last_month = month-i
            wweek,end = calendar.monthrange(year,last_month)
            # 如果返回月份的结束日期小于当前的day，要取该月份实际的day，如12月31日，返回11月的结束日期就是11月30
            first_day="%s-%s-01" %(year,last_month)
            last_day="%s-%s-%s" % (year,last_month,end)
            months_list.append((first_day,last_day))
    return months_list


def get_quarterdays_by_num(date_str,m):
    """
    返回当前日期所在季度，以及前M个季度的时间周期列表，实时和离线模块都可以使用
    :param date_str: 输入的时间字符串
    :param m: 计算前几个季度
    :return: 当前季度和前M个季度的时间区间列表
    """
    date = datetime_fmt(date_str)
    day = date.day
    year = date.year
    month = date.month
    now_year=datetime.now().year
    quarter_list = []
    if year == now_year:
        for i in range(0, m):
            last_month = month-i*3
            wweek, end=calendar.monthrange(year, last_month)
            if end < day:
                if 10 <= last_month <= 12:
                    first_day = "%s-%s-01" % (year,10)
                    last_day = "%s-%s-%s" % (year,last_month,end)
                elif 7 <= last_month < 10:
                    first_day = "%s-%s-01" % (year,7)
                    last_day="%s-%s-%s" % (year,last_month,end)
                elif 4 <= last_month < 7:
                    first_day = "%s-%s-01" % (year,4)
                    last_day="%s-%s-%s" % (year,last_month,end)
                elif 1 <= last_month < 4:
                    first_day = "%s-%s-01" % (year,1)
                    last_day="%s-%s-%s" % (year,last_month,end)
            else:
                if 10 <= last_month <= 12:
                    first_day = "%s-%s-01" % (year,10)
                    last_day = "%s-%s-%s" % (year,last_month,day)
                elif 7 <= last_month < 10:
                    first_day = "%s-%s-01" % (year,7)
                    last_day="%s-%s-%s" % (year,last_month,day)
                elif 4 <= last_month < 7:
                    first_day = "%s-%s-01" % (year,4)
                    last_day="%s-%s-%s" % (year,last_month,day)
                elif 1 <= last_month < 4:
                    first_day = "%s-%s-01" % (year,1)
                    last_day="%s-%s-%s" % (year,last_month,day)
            quarter_list.append((first_day,last_day))
    else:
        for i in range(0, m):
            last_month = month-i*3
            wweek, end=calendar.monthrange(year, last_month)

            if 10 <= last_month <= 12:
                first_day = "%s-%s-01" % (year,10)
                last_day = "%s-%s-%s" % (year,last_month,end)
            elif 7 <= last_month < 10:
                first_day = "%s-%s-01" % (year,7)
                last_day="%s-%s-%s" % (year,last_month,end)
            elif 4 <= last_month < 7:
                first_day = "%s-%s-01" % (year,4)
                last_day="%s-%s-%s" % (year,last_month,end)
            elif 1 <= last_month < 4:
                first_day = "%s-%s-01" % (year,1)
                last_day="%s-%s-%s" % (year,last_month,end)
            quarter_list.append((first_day,last_day))
    return quarter_list

def get_date_str(start,end,date_type,flag):
    """
    :param end: 传入周月季的最后一天
    :return: 根据传入的时间参数，进行日周月季各个维度的sql计算时间
    """
    if date_type == 'd':
        date_str = get_day_str(start, end)
    elif date_type == 'w':
        date_str = get_week_str(end,flag)
    elif date_type == 'm':
        date_str = get_month_str(end,flag)
    elif date_type == 'q':
        date_str = get_quarter_str(end,flag)
    return date_str


# 下面get_date_str |get_week_str|get_month_str|get_quarter_str 几个函数是给trend下钻页使用的
def get_day_str(start,end):
    """ 返回日的近7天查询时间"""
    """ 拼接date_str的字符串"""
    return "date_str >=" + "'" + start + "'" + " " + "and" + " " + "date_str <=" + " " + "'" + end + "'"


def get_week_str(end,flag):
    """ 返回当周近7周的查询时间"""
    if flag != 'trend':
        weeks_list = get_weekdays_by_num(end, 1)
    else:
        weeks_list=get_weekdays_by_num(end, 7)
    date_str = ''
    for i in range(len(weeks_list)):
        date_str += "date_str between " + "'" + weeks_list[i][0] +"'" + ' and ' + "'" + weeks_list[i][1] +"'" \
                  + ' ' + "or" + ' '
    # 字符串末尾有一个多余的or，用切片去掉后返回
    return date_str[:-3]


def get_month_str(end, flag):
    """
    返回当前月和前几个月的时间查询字符串
    :param end: 返回当前查询月份的的结束日期
    :param field_str:
    :return:
    """
    if flag != 'trend':
        month_list=get_monthdays_by_num(end, 1)
    else:
        month_list=get_monthdays_by_num(end, 7)
    date_str=''
    for i in range(len(month_list)):
        date_str+="date_str between " + "'" + month_list[i][0] + "'" + ' and ' + "'" + month_list[i][1] + "'" \
                  + ' ' + "or" + ' '
    # 字符串末尾有一个多余的or，用切片去掉后返回
    return date_str[:-3]


def get_quarter_str(end, flag):
    if flag != 'trend':
        quarter_list=get_quarterdays_by_num(end, 1)
    else:
        quarter_list=get_quarterdays_by_num(end, 4)
    date_str=''
    for i in range(len(quarter_list)):
        date_str+="date_str between " + "'" + quarter_list[i][0] + "'" + ' and ' + "'" + quarter_list[i][1] + "'" \
                  + ' ' + "or" + ' '
    # 字符串末尾有一个多余的or，用切片去掉后返回
    return date_str[:-3]


def get_week_by_date(date):
    """
    返回trend下钻页需要的key格式 如 W52
    :param date: 传入一个数据库查询到的日期，类型为datetime.date
    :return: 返回trend下钻页的数据格式   W20|W21|w22
    """
    # 计算当前日期在本年度的第多少周
    now_week=date.isocalendar()[1]
    w_order= 'W' + str(now_week)
    return w_order


def get_month_by_date(date):
    """
    返回trend下钻页需要的key格式 如 W52
    :param date: 传入一个数据库查询到的日期，类型为datetime.date
    :return: 返回trend下钻页的数据格式   W20|W21|w22
    """
    # 计算当前日期在本年度的第多少周
    month = date.month
    m_str = str(month) + '月'
    return m_str


def get_quarter_by_date(date):
    """
    返回trend下钻页需要的key格式 如 W52
    :param date: 传入一个数据库查询到的日期，类型为datetime.date
    :return: 返回trend下钻页的数据格式   W20|W21|w22
    """
    # 计算当前日期在本年度的第多少周
    month = date.month
    if 1 <= month < 4:
        q_str = 'Q1'
    elif 4 <= month < 7:
        q_str = 'Q2'
    elif 7 <= month < 10:
        q_str = 'Q3'
    elif 10 <= month <= 12:
        q_str = 'Q4'
    return q_str

if __name__ == '__main__':
    # print(get_special_date('2020-08-09'))
    # print(get_hours_list())
    # print(get_hours_list())
    # print(compute_before_date('2020-10-01', 300))
    # print(get_city_name('14614'))
    # print(get_week_order('2020-12-30'))
    # print(get_weekdays_by_num('2020-12-26', 2))
    # get_monthdays_by_num('2020-12-26', 4)
    #print(get_quarterdays_by_num('2020-10-15', 4))
    # print(get_quarterdays_by_num('2020-12-11',1))
    date_str ='2020-9-30'
    print(get_quarter_by_date(date_str))

