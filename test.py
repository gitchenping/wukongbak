from utils import db,util,log
from concurrent import futures
from multiprocessing import Manager

#ck 连接
# conn_ck = db.connect_clickhouse(host='10.0.5.80')
ck_db={
    'host':"http://10.0.5.80:8123",
    'headers':{'X-ClickHouse-User': 'membersbi', 'X-ClickHouse-Key': 'dangdangbi'}
}
conn_ck=db.CK(ck_db)

report = log.set_logger('applet_test.txt')

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
    table = 'bi_mdata.dwd_channel_mini_wechat_detail_d'

    where = '''distinct_id='{distinctid}' and permanentid='{permanentid}' 
                and creation_time='{creationtime}' and url='{url}' and data_date='{date}'
            '''.format(distinctid=item[itemfilter.index('distinct_id')],permanentid = item[itemfilter.index('permanentid')],
                  creationtime=item[itemfilter.index('creation_time')],url=item[itemfilter.index('url')],date=item[itemfilter.index('data_date')])

    ck_sql = "select "+columns+" from "+table+" where "+where


    ck_data = conn_ck.ck_get(ck_sql)
    if ck_data !=[]:
        dev_result = dict(zip(result_key,ck_data[0]))
    else:
        dev_result = {}

    diff_result = util.simplediff(test_result,dev_result)

    return where,diff_result,lock


def task_after(res):
    r= res.result()

    if r[1] !={}:
        r[-1].acquire()
        report.info(r[0]+'- Fail ')
        report.info('--diff info : '+str(r[1])+"\n")

        r[-1].release()

def test():
    '''渠道明细'''
    itemfilter =['distinct_id', 'permanentid', 'creation_time', 'url', 'platform', 'data_date', 'union_id', 'allianceid',
                  'channel1', 'channel2', 'channel3', 'channel4', 'channel5', 'row_num', 'is_first_visit', 'is_new_visit']
    df_2= [
        ['oFU4G0QPjH8PWyow8uqm9H8goJp8', '20210704150737636168011152943356777', '2021-07-04 15:22:49', '/pages/checkout/addressEdit?orderSequenceIds=0_0,10817_0&ref=checkout&requestParams={}', '4', '2021-07-04', 'null', '', 'null', 'null', 'null', 'null', 'null', 32.0, '2', '2'],
        ['oFU4G0QPjH8PWyow8uqm9H8goJp8', '20210704150737636168011152943356777', '2021-07-04 15:20:51', '/pages/cart/cart', '4', '2021-07-04', 'null', '', 'null', 'null', 'null', 'null', 'null', 10.0, '2', '2'],
        ['oFU4G0QPjH8PWyow8uqm9H8goJp8', '20210704150737636168011152943356777', '2021-07-04 15:23:17', '/pages/checkout/checkout', '4', '2021-07-04', 'null', '', 'null', 'null', 'null', 'null', 'null', 33.0, '3', '2'],
        ['oFU4G0QPjH8PWyow8uqm9H8goJp8', '20210704150737636168011152943356777', '2021-07-04 15:19:15', '/pages/cart/cart', '4', '2021-07-04', 'null', '', 'null', 'null', 'null', 'null', 'null', 3.0, '2', '2'],
        ['oFU4G0QPjH8PWyow8uqm9H8goJp8', '20210704150737636168011152943356777', '2021-07-04 15:57:40', '/pages/cart/cart', '4', '2021-07-04', 'null', '', 'null', 'null', 'null', 'null', 'null', 51.0, '2', '2'],
        ['oFU4G0QPjH8PWyow8uqm9H8goJp8', '20210704150737636168011152943356777', '2021-07-04 15:21:51', '/pages/search/index?cid=0&keyword=将军胡同', '4', '2021-07-04', 'null', '', 'null', 'null', 'null', 'null', 'null', 25.0, '2', '1'],
        ['oFU4G0QPjH8PWyow8uqm9H8goJp8', '20210704150737636168011152943356777', '2021-07-04 15:20:53', '/pages/index/index', '4', '2021-07-04', 'null', '', 'null', 'null', 'null', 'null', 'null', 11.0, '2', '2'],
        ['oFU4G0QPkKX770XoxYUfSH5TUnhA', '20210704043427910410980953107673114', '2021-07-04 04:40:53', '/pages/index/index', '4', '2021-07-04', 'null', '', 'null', 'null', 'null', 'null', 'null', 2.0, '2', '1'],
        ['oFU4G0QPkKX770XoxYUfSH5TUnhe', '20210704043427910410980953107673114', '2021-07-04 04:34:31', '/pages/product/product?unionid=P-337647m&cps_package=OMohcaoVjhyjjtvSokfd0EPGreibskf0AfBYl4QAhYu_PGD9r3lHeR9jy0Bp1N09&pid=25229369', '4', '2021-07-04', 'P-337647m', '337647', '联盟', '微信小程序', 'null', 'null', 'null', 1.0, '1', '1']
        ]

    workers = 2
    lock = Manager().Lock()
    # 'distinct_id','permanentid','creation_time','url','data_date'

    # for item in df_2:
    #     task(item, itemfilter, lock)

    with futures.ProcessPoolExecutor(workers) as executor:
        for item in df_2:
            future = executor.submit(task, item,itemfilter,lock)
            future.add_done_callback(task_after)



if __name__=="__main__":
    test()