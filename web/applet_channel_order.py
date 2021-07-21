'''
    微信小程序渠道分析-订单

'''
from utils import db,util,log,date
import pandas as pd
import os
import pickle
import time
import json
from concurrent import futures
from multiprocessing import Manager
from db.dao.applet_channel_order import wechat_order_detail_create_sql


#logger
report = log.set_logger('applet_order.txt')
# hive连接
# hive_cursor= db.connect_hive()
#ck 连接
#conn_ck = db.connect_clickhouse(host='10.0.5.80')
ck_db={
    'host':"http://10.0.5.80:8123",
    'headers':{'X-ClickHouse-User': 'membersbi', 'X-ClickHouse-Key': 'dangdangbi'}
}
ck_conn=db.CK(ck_db)

#diff task
def task(item,itemfilter,lock):

    result_key = ['channel1',
                   'channel2',
                   'channel3',
                   'channel4',
                   'channel5',
                   'union_id',
                   'is_first_visit',
                   'is_new_visit']
    temp = []
    for key in result_key:
        temp.append(item[itemfilter.index(key)])

    test_result = dict(zip(result_key,temp))

    columns = ','.join(result_key)
    table = 'bi_mdata.dwd_channel_mini_wechat_order_detail'

    where = '''distinct_id='{distinctid}' and permanentid='{permanentid}' and creation_time='{creationtime}' 
                and url='{url}' and data_date='{date}'
            '''.format(distinctid=item[itemfilter.index('distinct_id')],permanentid = item[itemfilter.index('permanentid')],
                  creationtime=item[itemfilter.index('creation_time')],url=item[itemfilter.index('url')],date=item[itemfilter.index('data_date')])

    ck_sql = "select "+columns+" from "+table+" where "+where
    ck_data = ck_conn.ck_get(ck_sql)

    dev_result = []
    if ck_data !=[]:

        for ele in ck_data:
            dev_result.append(dict(zip(result_key,ele)))
    else:
        dev_result.append({})

    for ele in dev_result:
        diff_result = util.simplediff(test_result,ele)
        if diff_result == {}:
            break

    return where,diff_result,lock


def task_after(res):
    r= res.result()

    if r[1] !={}:
        r[-1].acquire()
        report.info(r[0]+'- Fail ')
        report.info('--diff info : '+str(r[1]))
        r[-1].release()


def test_applet_channel_order():

    data_date = '2021-07-05'

    week_monday = date.get_startdate_in_w_m_q(data_date,'w')
    quarter_day = date.get_startdate_in_w_m_q(data_date,'q')

    last2month = date.get_lastdate_in_w_m_q(data_date,'m',2)
    lastyear = date.get_lastdate_in_w_m_q(data_date,'y',1)


    sql = wechat_order_detail_create_sql.format(date =data_date,week_monday = week_monday,
                                         quarter_day =quarter_day,last2month =last2month,
                                         lastyear =lastyear)

    if not os.path.exists("logs/mini_channel_order.pickle"):
        hive_cursor.execute(sql)
        order_data = hive_cursor.fetchall()
        with open('logs/mini_channel_order.pickle', 'wb') as f:
            pickle.dump(order_data, f)
    else:
        with open('logs/mini_channel_order.pickle', 'rb') as f:
            order_data = pickle.load(f)







