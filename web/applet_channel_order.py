'''
    微信小程序渠道分析-订单

'''
from utils import db,util,log,date
import os
import pickle
import time
import json
from concurrent import futures
from multiprocessing import Manager
from db.dao.applet_channel_order import wechat_order_detail_create_sql,wechat_order_detail_pay_sql,wechat_order_detail_send_sql


#logger
report = log.set_logger('applet_order.txt')
# hive连接
if os.name == 'posix':
    hive_cursor= db.connect_hive()
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

    if item_dict['order_status'] =='3':
        order_item_id = item_dict['order_id']
        order_item_name = 'order_id'
    else:
        order_item_id = item_dict['item_id']
        order_item_name = 'item_id'

    where = '''distinct_id='{distinctid}' and creation_time='{creationtime}' and {order_item_name}='{order_item_id}' and order_status='{order_status}' and data_date='{date}'
            '''.format(distinctid=item_dict['distinct_id'],creationtime=item_dict['creation_time'],order_item_name=order_item_name,
                  order_item_id=order_item_id,order_status=item_dict['order_status'],date=item_dict['data_date'])

    ck_sql = "select "+columns+" from "+table+" where "+where
    ck_data = ck_conn.ck_get(ck_sql)

    dev_result = []
    if ck_data !=[]:

        for ele in ck_data:
            temp = dict(zip(result_key,ele))
            temp['out_profit'] = round(float(temp['out_profit']),2)
            temp['out_pay_amount'] =round(float(temp['out_pay_amount']),2)
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
        report.info(r[0]+'-*-Fail-*- ')
        report.info('--diff info : '+str(r[1])+"\n")
        r[-1].release()


def test_applet_channel_order():

    data_date = '2021-07-05'

    week_monday = date.get_startdate_in_w_m_q(data_date,'w')
    quarter_day = date.get_startdate_in_w_m_q(data_date,'q')

    last2month = date.get_lastdate_in_w_m_q(data_date,'m',2)
    lastyear = date.get_lastdate_in_w_m_q(data_date,'y',1)


    order_dict={
                 # '收订':1,
                 #  '支付':2,
                 '出库':3

    }

    for key,value in order_dict.items():

        if key =='收订':

            sql = wechat_order_detail_create_sql
            data_dir ='mini_channel_create_order.pickle'
        elif key =='支付':

            sql = wechat_order_detail_pay_sql
            data_dir = 'mini_channel_pay_order.pickle'
        else:
            sql = wechat_order_detail_send_sql
            data_dir = 'mini_channel_send_order.pickle'


        if not os.path.exists("logs/"+data_dir):

            sql = sql.format(date=data_date, week_monday=week_monday,
                             quarter_day=quarter_day, last2month=last2month,
                             lastyear=lastyear)
            hive_cursor.execute(sql)
            order_data = hive_cursor.fetchall()
            with open('logs/'+data_dir, 'wb') as f:
                pickle.dump(order_data, f)
        else:
            with open('logs/'+data_dir, 'rb') as f:
                order_data = pickle.load(f)

        workers = 2
        lock = Manager().Lock()


        #单进程
        # for item in order_data:
        #     item_zip = zip(mini_wechat_order_detail_table.keys(), item)
        #     r=task(item_zip, lock)
        #     if r[1] !={}:
        #         r[-1].acquire()
        #         report.info(r[0]+'- Fail ')
        #         report.info('--diff info : '+str(r[1]))
        #         r[-1].release()



        #多进程
        with futures.ProcessPoolExecutor(workers) as executor:
            for item in order_data:
                item_zip = zip(mini_wechat_order_detail_table.keys(), item)
                future = executor.submit(task, item_zip, lock)
                future.add_done_callback(task_after)







