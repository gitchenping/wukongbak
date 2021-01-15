import time
import datetime
from .date import *

'''计算同环比'''
def tb_hb_cal(rawdata):
    '''同比、环比计算，返回一个二维列表'''
    newdata=[]
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
    newdata[0] = [round(ele, 2) for ele in currentvaluelist[:-1] if ele is not None]+[currentvaluelist[-1]]   #首行处理
    return newdata



'''获取同比、环比key'''
def get_tb_hb_key(data,date,datetype):
    '''

    :param data:       二维列表，
    :param date:
    :param datetype:
    :return:
    '''

    lastestdate = get_startdate_in_w_m_q(date,datetype)

    #默认keylist
    keylist = ['value', '环比', '同比去年']
    if datetype == 'day' or datetype == 'd':
        keylist = ['value', '环比', "同比上周", '同比去年']

    length_raw = len(data)
    data_datelist = [ele[-1] for ele in data]
    if length_raw !=len(keylist):                               # 说明有同比或环比忽略

        if lastestdate > data[0][-1] :                            # 如果没有最近日期的数据，无法计算同比、环比
            keylist=[]                                           #keylist为空，说明不能进行同比、环比计算
        else:
            # 判断同比、环比计算那个或两个都计算
            keylist = ['value']

            begindate_str = data_datelist[0]                         #第一个日期
            begindate_date = datetime.datetime.strptime(begindate_str, '%Y-%m-%d')

            for i in range(1, len(data_datelist)):
                next_date = datetime.datetime.strptime(data_datelist[i], '%Y-%m-%d')

                deltadays = (begindate_date - next_date).days

                if datetype == 'day' or datetype == 'd':
                    if deltadays == 1:
                        keylist.append('环比')
                    elif deltadays > 1 and deltadays < 8:
                        keylist.append('同比上周')
                    else:
                        keylist.append('同比去年')

                elif datetype == 'wtd' or datetype == 'w':
                    if deltadays > 7:
                        keylist = ['value', '同比去年']
                    else:
                        keylist = ['value', '环比']
                elif datetype == 'mtd' or datetype == 'm':

                    if deltadays > 31:
                        keylist = ['value', '同比去年']
                    else:
                        keylist = ['value', '环比']

                else:
                    if deltadays > 92 and (datetype == 'qtd' or datetype == 'q'):
                        keylist = ['value', '同比去年']
                    else:
                        keylist = ['value', '环比']

    return keylist