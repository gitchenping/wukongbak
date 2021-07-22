
from utils import log,db

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

#ck 连接
#conn_ck = db.connect_clickhouse(host='10.0.5.80')
ck_db={
    'host':"http://10.0.5.80:8123",
    'headers':{'X-ClickHouse-User': 'membersbi', 'X-ClickHouse-Key': 'dangdangbi'}
}
ck_conn=db.CK(ck_db)

mini_wechat_order_detail_table={
    'distinct_id' : '设备标示-统一字段',
     'creation_time' : '埋点时间',
     'union_id' : '渠道id',
     'channel1' : '一级渠道',
     'channel2' : '二级渠道',
     'channel3' : '三级渠道',
     'channel4' : '四级渠道',
     'channel5' : '五级渠道',
     'cust_id' : '客户id',
     'parent_id' : '订单id',
     'order_id' : '包裹id',
     'item_id' : '订单明细主键',
     'out_profit' :'净销售额',
     'out_pay_amount' :'实付金额',
     'day_new_flag' : '订单新客-日',
     'week_new_flag' : '订单新客-周',
     'month_new_flag' : '订单新客-月',
     'quarter_new_flag' : '订单新客-季度',
     'platform' : '平台',
     'order_status' : '状态，1：收订，2：支付，3：出库',
     'sale_type' : '销退标识（只有计算净销售额和实付的时候统计2）：1:销售 2：销退 3:回函',
     'bd_id' : '事业部id：1-出版物 2-日百 3-数字 4-文创 5-其它',
     'data_date' : '日期'
}


#diff task
def task(itemzip,lock):

    item_dict = dict(itemzip)
    result_key = ['channel1',
                   'channel2',
                   'channel3',
                   'channel4',
                   'channel5',
                   'union_id',
                   'out_profit',
                   'out_pay_amount',
                   'day_new_flag',
                  'week_new_flag',
                  'month_new_flag',
                  'month_new_flag'
                  ]

    test_result = {}
    for key in result_key:
        test_result[key] = item_dict[key]

    columns = ','.join(result_key)
    table = 'bi_mdata.dwd_channel_mini_wechat_order_detail'

    where = '''distinct_id='{distinctid}' and creation_time='{creationtime}' 
                and item_id='{item_id}' and order_status='{order_status}' and data_date='{date}'
            '''.format(distinctid=item_dict['distinct_id'],creationtime=item_dict['creation_time'],
                  item_id=item_dict['item_id'],order_status=item_dict['order_status'],date=item_dict['data_date'])

    ck_sql = "select "+columns+" from "+table+" where "+where
    ck_data = ck_conn.ck_get(ck_sql)

    dev_result = []
    if ck_data !=[]:

        for ele in ck_data:
            temp = dict(zip(result_key, ele))
            temp['out_profit'] = round(float(temp['out_profit']), 2)
            temp['out_pay_amount'] = round(float(temp['out_pay_amount']), 2)
            dev_result.append(temp)

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

    order_data =[('20210623191119422510345696493832042', '2021-07-05 13:47:41', 'p-324867m-397-20', '微信公众号', '服务号', '菜单栏', '', '', '707764162', '42862811212', '42862811212', '42862811212001', 0.0, 0.0, '2', '2', '2', '2', '4', '1', '1', '5', '2021-07-05'),
                 ('20200213103907790737186704977675237', '2021-07-05 14:34:25', 'p-324867m-397-20', '微信公众号', '服务号', '菜单栏', '', '', '312446630', '42862786210', '42862786210', '42862786210001', 0.0, 0.0, '2', '2', '2', '2', '4', '1', '1', '5', '2021-07-05'),
                 ('20210705213330348595200698797379680', 'null', 'null', '自然量', 'null', 'null', 'null', 'null', '338105705', '42862782655', '42862782655', '42862782655001', 0.0, 0.0, '2', '2', '2', '2', '4', '1', '1', '12', '2021-07-05'),
                 ('20210620221547249883562833435216982', '2021-07-05 14:12:45', 'p-337647m', '联盟', '微信小程序', 'null', 'null', 'null', '29350166', '42862779156', '42862779156', '42862779156007', 0.0, 0.0, '2', '2', '2', '2', '4', '1', '1', '5', '2021-07-05'),
                 ('20210705213926865327650917819696787', '2021-07-05 13:39:28', 'p-121416269m', '联盟', '评论联盟', 'null', 'null', 'null', '728557437', '42862749437', '42862749437', '42862749437003', 0.0, 0.0, '2', '1', '1', '1', '4', '1', '1', '5', '2021-07-05')]

    workers = 2
    lock = Manager().Lock()
    # 'distinct_id','permanentid','creation_time','url','data_date'

    # for item in order_data:
    #
    #     item_zip = zip(mini_wechat_order_detail_table.keys(),item)
    #     task(item_zip,lock)

    with futures.ProcessPoolExecutor(workers) as executor:
        for item in order_data:
            item_zip = zip(mini_wechat_order_detail_table.keys(),item)
            future = executor.submit(task, item_zip, lock)
            future.add_done_callback(task_after)



if __name__=="__main__":
    test_applet_channel_order()

