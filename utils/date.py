import time
import datetime
import math

'''获取月份所在的季度起始日期'''
def get_sd_ed_q(m):
    m=int(m)
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

'''获取当前日期所在的起始周、月、季的起始日期'''
def get_startdate_in_w_m_q(date,datetype):
    # date=data['date_str']               #'2020-xxx-xx'
    # datetype=data['date_type']

    templist=date.split('-')
    year=templist[0]
    m=templist[1]
    d=templist[2]

    startdate=None
    _date = datetime.datetime.strptime(date, '%Y-%m-%d')

    if datetype.startswith('d'):                            #datetype='day' or 'd'
        startdate = date
    elif datetype.startswith('w'):
        a=_date.weekday()
        startdate=datetime.datetime.strftime(_date - datetime.timedelta(days=a),'%Y-%m-%d')
    elif datetype.startswith('m'):
        startdate=year+"-"+m+"-"+"01"
    else:
        startdate=str(year)+get_sd_ed_q(m)[0]
    return startdate
