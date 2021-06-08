import time
import datetime
import math
import calendar

#返回实时时间，精确到小时
def get_realtime():
    '''

    :return: 2021-06-01 13
    '''
    datetimenow = datetime.datetime.now()
    year = str(datetimenow.year)
    month = str(datetimenow.month)
    day = str(datetimenow.day)
    hour = str(datetimenow.hour - 1)

    date_str = year + "-" + (len(month) < 2 and '0' + month or month) + "-" + (
            len(day) < 2 and '0' + day or day)
    hour_str = len(hour) < 2 and '0' + hour or hour

    return date_str+" "+hour_str


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

'''获取当前日期所在的周、月、季的起始日期'''
def get_startdate_in_w_m_q(date,datetype):
    # date=data['date_str']               #'2020-xxx-xx'
    # datetype=data['date_type']

    templist=date.split('-')
    year=templist[0]
    m=templist[1]

    startdate=None
    _date = datetime.datetime.strptime(date, '%Y-%m-%d')

    if datetype.startswith('d') or datetype.startswith('h'):                            #datetype='day' or 'd'
        startdate = date
    elif datetype.startswith('w'):
        a=_date.weekday()
        startdate=datetime.datetime.strftime(_date - datetime.timedelta(days=a),'%Y-%m-%d')
    elif datetype.startswith('m'):
        startdate=year+"-"+m+"-"+"01"
    else:
        startdate=str(year)+get_sd_ed_q(m)[0]
    return startdate

'''返回月末最后一天'''
def get_month_end_day(date):
    a=datetime.datetime.strptime(date, '%Y-%m-%d')
    this_month_end = datetime.datetime(a.year, a.month, calendar.monthrange(a.year, a.month)[1])

    return datetime.datetime.strftime(this_month_end,'%Y-%m-%d')

'''获取日期所在月的总天数'''
def get_totaldays(year,month):
    month=str(month)
    if month=='2':
        # 闰年 2月份有29天
        a=int(year)
        if  a % 400==0 or (a % 4==0 and a % 100!=0):
            totaldays=29
        else:
            totaldays=28
    elif month in ['04','06','09','11','4','6','9'] :
        totaldays=30
    else:
        totaldays=31
    return totaldays

'''输出日期所在的月初和月末'''
def get_day2month(year,month):

    if month=='0':
        new_month='12'
        new_year=int(year)-1
    else:
        new_month=month
        new_year=year

    if new_month=='2':
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

    new_month=len(str(new_month))<2 and '0'+str(new_month) or str(new_month)
    return str(new_year)+"-"+new_month+"-01",str(year)+"-"+str(month)+"-"+lastday

def get_day2q(m):
    _m=int(m)
    tmp=math.ceil(_m / 3)
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

'''获取当前日期所在的周、月、季的结束日期'''
def get_enddate_in_w_m_q(date,datetype):
    templist = date.split('-')
    year = templist[0]
    m = templist[1]
    d = templist[2]

    enddate = None
    _date = datetime.datetime.strptime(date, '%Y-%m-%d')

    if datetype.startswith('d'):                            #datetype='day' or 'd'
        enddate = date
    elif datetype.startswith('w'):
        a=_date.weekday()                                  #周几--周一是0，周日是6
        enddate=datetime.datetime.strftime(_date + datetime.timedelta(days=6-a),'%Y-%m-%d')
    elif datetype.startswith('m'):
        totaldays=get_totaldays(year,m)
        enddate=year+"-"+m+"-"+str(totaldays)
    else:
        enddate = str(year) + get_sd_ed_q(m)[1]

    return enddate

'''获取指定日期的环比、同步起始日期'''
def get_tb_hb_date(date,datetype):
    tb_hb_date=None

    if datetype.startswith('h'):
        date=date.split(' ')[0]
    datelist=date.split('-')

    _year=datelist[0]
    _month=datelist[1]
    _day=datelist[2]

    year=int(_year)
    month=int(_month)
    day=int(_day)

    _date = datetime.datetime.strptime(date, '%Y-%m-%d')                      #hb\tb往前推

    #当前日期所在的日、周、月、季起始日期
    date_s_str=get_startdate_in_w_m_q(date, datetype)
    date_e_str=get_enddate_in_w_m_q(date, datetype)

    date_s_date=datetime.datetime.strptime(date_s_str, '%Y-%m-%d')
    date_e_date=datetime.datetime.strptime(date_e_str, '%Y-%m-%d')

    #离线业务最近一天
    _latest_date=datetime.datetime.today() - datetime.timedelta(days=1)

    if _latest_date < date_e_date:
        final_date_e = _latest_date        # 不完整的一周、一月、一季
    else:
        final_date_e=date_e_date

    #时间范围内有多少天
    delta_day=final_date_e-date_s_date

    if datetype == 'day' or datetype == 'd' or datetype=='h':     #时、天
        #今天

        # 昨天
        hb_date= datetime.datetime.strftime(_date - datetime.timedelta(days=1),'%Y-%m-%d')
        # 上周的今天
        tb_week_date = datetime.datetime.strftime(_date - datetime.timedelta(days=7),'%Y-%m-%d')
        # 去年今天
        tmp_year=year-1
        tb_year_date=str(tmp_year)+"-"+_month+"-"+(len(str(day))<2 and '0'+str(day) or str(day))

        #合并
        tb_hb_date=(date,hb_date,tb_week_date,tb_year_date)
    elif datetype=='wtd' or datetype == 'w':
        # 本周
        week_s=date_s_str
        week_e=datetime.datetime.strftime(final_date_e,'%Y-%m-%d')

        #上周
        hb__week_date_s = date_s_date - datetime.timedelta(days=7)
        hb__week_date_e = hb__week_date_s+delta_day

        #去年
        year_weeknum_weekday = _date.isocalendar()        #哪一年、第几周、周几
        last_year=year_weeknum_weekday[0]-1
        weeknum=year_weeknum_weekday[1]
        a = time.strptime(str(last_year) + "-" + str(weeknum) + '-0', '%Y-%U-%w')
        tb_year_date_sunday=datetime.datetime.strptime(str(a.tm_year)+"-"+str(a.tm_mon)+"-"+str(a.tm_mday),'%Y-%m-%d')

        tb_year_date_s=tb_year_date_sunday-datetime.timedelta(days=6)     #周一

        tb_year_date_e=tb_year_date_s+delta_day
        #汇总
        # tb_hb_date = ((week_s,week_e),
        #               (datetime.datetime.strftime(hb__week_date_s, '%Y-%m-%d'),datetime.datetime.strftime(hb__week_date_e, '%Y-%m-%d')),
        #               (datetime.datetime.strftime(tb_year_date_s, '%Y-%m-%d'),datetime.datetime.strftime(tb_year_date_e, '%Y-%m-%d')))
        tb_hb_date = ((week_s, week_e),
                      (datetime.datetime.strftime(hb__week_date_s, '%Y-%m-%d'),datetime.datetime.strftime(hb__week_date_e, '%Y-%m-%d'))
                      )
    elif datetype=='mtd' or datetype == 'm':
        # 本月
        month_s=date_s_str
        month_e=datetime.datetime.strftime(final_date_e,'%Y-%m-%d')
        # 环比上月

        month -= 1
        hb_month_date_s = get_day2month(str(year), str(month))[0]
        hb_month_date_e=datetime.datetime.strptime(hb_month_date_s,'%Y-%m-%d')+delta_day

        # 同比去年
        month = month + 1
        last_year = year - 1
        tb_year_month_s = get_day2month(str(last_year), str(month))[0]
        tb_year_month_e = datetime.datetime.strptime(tb_year_month_s, '%Y-%m-%d') + delta_day

        #汇总
        tb_hb_date = (
        (month_s, month_e),               #当期
        (hb_month_date_s,datetime.datetime.strftime(hb_month_date_e, '%Y-%m-%d')),      #环比基期
        (tb_year_month_s, datetime.datetime.strftime(tb_year_month_e, '%Y-%m-%d')))     #同比基期
    else:
        # 本季度
        q_s=date_s_str
        q_e=datetime.datetime.strftime(final_date_e,'%Y-%m-%d')

        # 环比上季度
        hb_month = month -3
        if hb_month<0:
            hb_month=10
            last_year = year - 1
        else:
            last_year=year
        hb_q_date_s=str(last_year)+get_day2q(hb_month)[0]

        #环比按日期取即可
        # hb_q_date_e=datetime.datetime.strptime(hb_q_date_s,'%Y-%m-%d')+delta_day
        hb_q_date_e=datetime.datetime.strptime(_year+"-0"+str(month-3)+"-"+_day,'%Y-%m-%d')

        # 同比去年
        m = month
        y = str(year - 1)
        tmp = get_day2q(m)
        tb_q_date_s = y + tmp[0]
        tb_q_date_e=datetime.datetime.strptime(tb_q_date_s,'%Y-%m-%d')+delta_day

        tb_hb_date=((q_s,q_e),
                    (hb_q_date_s,datetime.datetime.strftime(hb_q_date_e, '%Y-%m-%d')),
                    (tb_q_date_s,datetime.datetime.strftime(tb_q_date_e, '%Y-%m-%d')))

    return tb_hb_date

def datechange(type,enddate):
    '''返回日期所在的日、周、月、季key for api'''
    templist = enddate.split('-')
    date_type=type
    # date转化
    key=''
    if date_type == 'day':
        key = enddate
    elif date_type == 'wtd' or date_type == 'w':
        a = datetime.datetime.strptime(enddate, '%Y-%m-%d')
        key = templist[0] + '-w' + str(a.isocalendar()[1])
    elif date_type == 'mtd' or date_type == 'm' :
        key = templist[0] + '-m' + str(int(templist[1]))
    else:
        key = templist[0] + '-q' + str(math.ceil(int(templist[1]) / 3))
    return key

'''trend键值'''
def get_trend_where_date(data):
    datetype=data['date_type']
    datestr=data['date']

    datelist=[]
    wheredate=''
    wheredata=''

    templist=datestr.split('-')

    if datetype.startswith('h'):     #小时的话只取当天
        wheredate=datestr.split(' ')
        date_1=wheredate[0]
        date_2=wheredate[1]
        wheredata = " and date_str in ('" + date_1 + "') and hour_str<='"+str(date_2)+"'"

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

    if datetype == 'wtd' or datetype == 'w':  # 最近七周

        # 将当前日期换算到周日

        datestr=get_enddate_in_w_m_q(datestr, datetype)
        end_date_datetime = datetime.datetime.strptime(datestr, '%Y-%m-%d')
        i=0
        while i < 7:

            tempdatetime_e=end_date_datetime-datetime.timedelta(days=7 * i)
            tempdatetime_s=tempdatetime_e-datetime.timedelta(days=6)

            wheredate += " t.date_str between '" +tempdatetime_s.strftime("%Y-%m-%d") + "' and '" + tempdatetime_e.strftime("%Y-%m-%d") + "' or "

            i+=1

        wheredata = ' and (' + wheredate.strip('or ')+")"

        pass

    if datetype == 'mtd' or datetype == 'm':  # 最近七个月
        year=templist[0]
        month=templist[1]
        _month=int(month)

        i=0
        while i<min(7,_month):
            month_s_e=get_day2month(year,month)

            wheredate += " t.date_str between '" + month_s_e[0] + "' and '" +month_s_e[1] + "' or "

            month=str(int(month)-1)
            i+=1

        wheredata = ' and ('+ wheredate.strip('or ')+")"

        pass

    if datetype == 'qtd' or datetype == 'q':  # 最近3个季度
        year = templist[0]
        month = templist[1]

        i=0
        while i< math.ceil(int(month)/3+1)-1:
            a=int(month)-3*i
            q_s_e =get_day2q(a)

            wheredate += " t.date_str between '" +year+ q_s_e[0] + "' and '" +year+ q_s_e[1] + "' or "
            i+=1

        wheredata = ' and (' + wheredate.strip('or ')+")"

    return wheredata

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


'''返回trend的格式化键值对'''
def get_trend_data(data,datetype):
    trend={}
    if datetype.startswith('h'):
        for ele in data:
            key=ele[1]+"点"
            trend[key]=round(float(ele[0]),2)

    elif datetype == 'day' or datetype=='d':

        for ele in data:
            datelist = ele[1].split('-')
            key = datelist[1] + "/" + datelist[2]
            trend[key] = round(float(ele[0]),2)

    elif datetype == 'wtd' or datetype == 'w':
        for ele in data:
            a = datetime.datetime.strptime(ele[1], '%Y-%m-%d')
            key = 'W' + str(a.isocalendar()[1])
            trend[key] = round(float(ele[0]),2)
    elif datetype == 'mtd' or datetype=='m':
        for ele in data:
            a=ele[1].split('-')[1]
            key=str(int(a)) + '月'
            trend[key] = round(float(ele[0]),2)
    else:
        for ele in data:
            a=ele[1].split('-')[1]
            key="Q"+str(int((int(a)-1)/3+1))
            trend[key] = round(float(ele[0]),2)

    return trend