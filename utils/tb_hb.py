import time
import datetime
from .date import *


tb_hb_name_dict = {
    'value_ori': 'value',      #首页使用
    'value':'value',         #下钻页使用
    'tb_week': '同比上周',
    'tb_year': '同比去年',
    'value_hb': '环比'
}

def tb_hb_format(data):
    if isinstance(data,str) and data.endswith('%'):
        return float(data.strip('%'))
    else:
        return data




'''根据日期类型获取同环比key'''
def get_tb_hb_key_dict(datetype,overview=True):
    '''

    :param datetype:
    :param overview: 默认首页tbhb键
    :return:
    '''
    keyitem = dict(tb_hb_name_dict)
    if overview:
        keyitem.pop('value')
    else:
        keyitem.pop('value_ori')

    if datetype == 'd' or datetype=='h':
        # keylist = keyitem.keys()
        pass
    elif datetype == 'w':
        keyitem.pop('tb_week')
        keyitem.pop('tb_year')
        # keylist = keyitem.keys()
    else:
        keyitem.pop('tb_week')
        # keylist = keyitem.keys()

    return keyitem



'''sql返回结果集获取同比、环比key'''
def get_tb_hb_key_list(data,date,datetype,tbhb_keydict):
    '''

    :param data:       二维列表，
    :param date:
    :param datetype:
    :return:
    '''
    if datetype.startswith('h'):
        date=date.split(' ')[0]

    lastestdate_str = get_startdate_in_w_m_q(date,datetype)     #date所在的日 周一、月一号、季度一号
    lastestdate_date=datetime.datetime.strptime(lastestdate_str, '%Y-%m-%d')


    length_raw = len(data)
    data_datelist = [ele[-1] for ele in data]

    keylist = []

    if length_raw!=0:

        begindate_str = data_datelist[0]  # 第一个日期
        begindate_date = datetime.datetime.strptime(begindate_str, '%Y-%m-%d')

        if lastestdate_date <= begindate_date:  # 如果没有最近日期的数据，无法计算同比、环比

            if tbhb_keydict.__contains__('value_ori'):
                keylist.append('value_ori')
            if tbhb_keydict.__contains__('value'):
                keylist.append('value')

            for i in range(1, length_raw):
                next_date = datetime.datetime.strptime(data_datelist[i], '%Y-%m-%d')

                deltadays = (begindate_date - next_date).days

                if datetype == 'day' or datetype == 'd' or datetype == 'h':
                    if deltadays == 1:
                        tbhbname='value_hb'
                    elif deltadays > 1 and deltadays < 8:
                        tbhbname='tb_week'
                    else:
                        tbhbname='tb_year'

                elif datetype == 'wtd' or datetype == 'w':
                    if deltadays > 7:
                        tbhbname='tb_year'
                    else:
                        tbhbname='value_hb'

                elif datetype == 'mtd' or datetype == 'm':

                    if deltadays > 31:
                        tbhbname='tb_year'
                    else:
                        tbhbname='value_hb'

                elif datetype == 'qtd' or datetype == 'q':
                    if deltadays > 92:
                        tbhbname='tb_year'
                    else:
                        tbhbname='value_hb'

                keylist.append(tbhbname)

    return keylist


'''计算同环比'''
def tb_hb_cal(rawdata,tb_hb_key_list):
    '''

    :param rawdata: [[360.0, '2021-05-12'], [198.0, '2021-05-05'], [216.0, '2020-05-12']]
    :param misskeyvalue:
    :return: 返回一个二维列表
    '''

    newdata = [ele[0] for ele in rawdata]

    first_value=newdata[0]
    newdata=[first_value]+[round((first_value - ele) / ele * 100, 2) for ele in newdata[1:] if ele is not None and ele!=0]

    newdata_dict=dict(zip(tb_hb_key_list,newdata))
    return newdata_dict