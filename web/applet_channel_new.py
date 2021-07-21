'''
    微信小程序渠道分析

'''
from utils import db,util,log
import pandas as pd
import os
import pickle
import time
import json
from concurrent import futures
from multiprocessing import Manager


#logger
report = log.set_logger('applet_new.txt')

# hive连接
hive_cursor= db.connect_hive()
#ck 连接
#conn_ck = db.connect_clickhouse(host='10.0.5.80')
ck_db={
    'host':"http://10.0.5.80:8123",
    'headers':{'X-ClickHouse-User': 'membersbi', 'X-ClickHouse-Key': 'dangdangbi'}
}
ck_conn=db.CK(ck_db)


#全局变量
liebian_urls = {}
unionids = {}
lianmeng_dict = {}
ziran_distinctid_dict ={}
openid_new_dict = {}
liebian_lanuchs ={"scene":[1007,1008,1048,1047,1155]}

#从hive表读取裂变和非裂变渠道字典
def get_liebian_unionid():
    global liebian_urls
    global unionids
    sql ='''
        select
            channel_id,
            concat(channel_name1,'|',COALESCE(channel_name2,''),'|',COALESCE(channel_name3,''),
            '|',COALESCE(channel_name4,''),'|',COALESCE(channel_name5,''))
        from 
            dim.wechat_channel
    '''
    hive_cursor.execute(sql)
    liebian_unionid_list = hive_cursor.fetchall()
    liebian_urls = {}
    unionids = {}
    for item in liebian_unionid_list:
        key = item[0]
        value = item[1]
        if value.startswith('分享裂变') :
            liebian_urls[key] = value
        else:
            unionids[key] = value


def get_lianmeng_data():
    global lianmeng_dict

    lianmeng_sql = '''
                    select
                        allianceid,
                        alliancename
                    from
                        ods.alliancemember
            '''
    # 联盟账户字典
    if not os.path.exists("logs/mini_lianmeng.pickle"):
        hive_cursor.execute(lianmeng_sql)
        lianmeng_dict = hive_cursor.fetchall()
        lianmeng_dict = dict(lianmeng_dict)
        with open('logs/mini_lianmeng.pickle', 'wb') as f:
            pickle.dump(lianmeng_dict, f)
    else:
        with open('logs/mini_lianmeng.pickle', 'rb') as f:
            lianmeng_dict = pickle.load(f)

def get_openid_new_data(data_date):
    # 新访openid
    global openid_new_dict

    mini_view_new_sql = '''
                   select 
                       openid,1 as new 
                   from 
                       dwd.flow_mini_view_new 
                   where data_date='{date}' and platform='wechat_mina'
           '''

    if not os.path.exists("logs/mini_view_new.pickle"):
        hive_cursor.execute(mini_view_new_sql.format(date=data_date))
        openid_new_dict = hive_cursor.fetchall()
        openid_new_dict = dict(openid_new_dict)
        with open('logs/mini_view_new.pickle', 'wb') as f:
            pickle.dump(openid_new_dict, f)
    else:
        with open('logs/mini_view_new.pickle', 'rb') as f:
            openid_new_dict = pickle.load(f)


#openid 是否为新用户
def openid_parse(distinctid):
    is_new_visit = '2'
    if openid_new_dict.__contains__(distinctid) and distinctid !='':
        is_new_visit = '1'
    return is_new_visit


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
    table = 'bi_mdata.dwd_channel_mini_wechat_detail_d'

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


def getunionidfromurl(url):
    union_id = 'null'
    _DDCLICKUNION = "_ddclickunion="
    UNIONID = "unionid="
    if url.find(_DDCLICKUNION) > 0:
        partlist = url.split(_DDCLICKUNION)
        partlist_2 = partlist[1].split('|')
        union_id = partlist_2[0]
    elif url.find(UNIONID) > 0:
        partlist = url.split(UNIONID)
        partlist_2 = partlist[1].split('&')
        union_id = partlist_2[0]
    return union_id.lower()

def getallianceid(union_id):
    allianceid = ''
    unions = union_id.split('-')
    if len(unions) >=2:
        if unions[0] == 'p':
            allianceid = unions[1]
        allianceid = allianceid.rstrip('m').strip()
    return allianceid

#union_id,allianceid 解析规则
def unionid_allianceid_parse(url) :
    union_id = getunionidfromurl(url)
    allianceid = getallianceid(union_id)
    return union_id,allianceid


#channel解析
def channel_parse(url_lanuchinfo):
    channel1 = 'null'
    channel2 = 'null'
    channel3 = 'null'
    channel4 = 'null'
    channel5 = 'null'
    is_liebian = False
    lanuch_info = url_lanuchinfo['lanuch_info']
    url = url_lanuchinfo['url']
    union_id = url_lanuchinfo['union_id']
    try:
        scene_info = json.loads(lanuch_info)['scene']
    except Exception as e:
        scene_info = None
    if scene_info in liebian_lanuchs['scene']:
        is_liebian = True
    # 如果是来源是裂变，
    if is_liebian:
        for key, value in liebian_urls.items():
            if url.startswith(key):
                channel = value
                channels = channel.split('|')
                channel1 = channels[0]
                channel2 = channels[1]
                channel3 = channels[2]
                channel4 = channels[3]
                channel5 = channels[4]
                break
    #渠道号直接获取(从长到短遍历获取渠道，获取到则退出循环)
    if union_id != 'null':
        channels = 'null'
        if not unionids.__contains__(union_id):
            union_id_part = union_id.split('-')
            while len(union_id_part) > 2:
                union_id_part.pop(-1)
                temp_union_id = '-'.join(union_id_part)
                if unionids.__contains__(temp_union_id):
                    channels = unionids[temp_union_id]
                    break
        else:
            channels = unionids[union_id]
        if channels != 'null':
            channels = channels.split('|')
            channel1 = channels[0]
            channel2 = channels[1]
            channel3 = channels[2]
            channel4 = channels[3]
            channel5 = channels[4]
    return channel1,channel2,channel3,channel4,channel5

#联盟渠道
def lianmeng_channel(channel_allianceid):
    channel1 = channel_allianceid['channel1']
    channel2 = channel_allianceid['channel2']
    allianceid = channel_allianceid['allianceid']
    if lianmeng_dict.__contains__(allianceid):
        alliancename = lianmeng_dict[allianceid]
        if channel1 == 'null' and alliancename != '':
            channel1 = '联盟'
            channel2 = alliancename
    return channel1,channel2

#自然流量
def ziran_parse(ziran):
    channel1 = ziran['channel1']
    distinctid = ziran['distinct_id']
    if ziran_distinctid_dict.__contains__(distinctid):
        temp = ziran_distinctid_dict[distinctid]
        if temp != 'null':
            channel1 = '自然量'
    return channel1



def test_channel_mina_detail(init_start=200000,init_end=201000):

    global ziran_distinctid_dict

    data_date='2021-07-05'

    #全局变量
    get_liebian_unionid()
    get_lianmeng_data()
    get_openid_new_data(data_date)


    flow_mini_view_sql = '''
                select 
                    openid as distinct_id,
                    permanentid,
                    unix_timestamp(`time`,'yyyy-MM-dd HH:mm:ss')  as creation_time,
                    url,
                    (case when platform='21' then '4' else platform end) as platform,
                    lanuch_info,
                    data_date 
                from 
                    dwd.flow_mini_view_detail 
                where 
                    data_date='{date}' and platform='21' and type='1'
                order by distinct_id
            '''

    if not os.path.exists("logs/mini_base.pickle"):
        hive_cursor.execute(flow_mini_view_sql.format(date=data_date))
        base_data = hive_cursor.fetchall()
        with open('logs/mini_base.pickle', 'wb') as f:
            pickle.dump(base_data, f)
    else:
        with open('logs/mini_base.pickle', 'rb') as f:
            base_data = pickle.load(f)

    columns = ['distinct_id', 'permanentid', 'creation_time', 'url', 'platform', 'lanuch_info', 'data_date']
    df = pd.DataFrame(base_data, columns=columns)
    # 数据量太大截断处理
    distinctid_list = df['distinct_id'].to_list()
    record_num = len(distinctid_list)

    if record_num <= init_start:
        endline_num = record_num
        startline_num = 0
    elif record_num > init_start and record_num <= init_end:

        endline_num = record_num
        startline_num = init_start
    else:
        startline_num = init_start
        endline_num = init_end

    # 截取完整的distinct_id
    did = distinctid_list[startline_num]
    i = startline_num - 1
    while i >= 0 and distinctid_list[i] == did:
        i -= 1
    startline_num = i + 1

    did = distinctid_list[endline_num]
    i = endline_num + 1

    while i < record_num and distinctid_list[i] == did:
        i += 1
    endline_num = i

    df_range = df[startline_num:endline_num].copy()

    #unionid\allianceid解析
    unionid_allianceid_series = df_range['url'].map(unionid_allianceid_parse)
    df_range[['union_id', 'allianceid']] = unionid_allianceid_series.apply(pd.Series)

    # 渠道
    temp_channel = df_range[['url', 'lanuch_info', 'union_id']].apply(channel_parse, axis=1)
    # 增加渠道列
    df_range[['channel1', 'channel2', 'channel3', 'channel4', 'channel5']] = temp_channel.apply(pd.Series)

    #联盟
    df_range = df_range.drop(['lanuch_info'], axis=1)  # 舍弃该列
    temp_channel = df_range[['channel1', 'channel2', 'allianceid']].apply(lianmeng_channel, axis=1)
    df_range[['channel1', 'channel2']] = temp_channel.apply(pd.Series)

    # 自然渠道的用户
    dfgroupby = df_range[['distinct_id', 'channel1']].groupby('distinct_id')
    for id, channel_df in dfgroupby:
        # 自然流量用户 （一级渠道为null 且只有一个一级渠道）
        temp = channel_df['channel1']
        if temp.nunique() == 1 and temp.unique()[0] == 'null':
            ziran_distinctid_dict[id] = 1

    # 自然量标识
    df_range['channel1'] = df_range[['distinct_id', 'channel1']].apply(ziran_parse, axis=1)

    # 排名
    df_range['creation_time'] = df_range['creation_time'].astype('int')
    df_range['row_num'] = df_range['creation_time'].groupby(df['distinct_id']).rank(method='first')  # 分组排序

    # 首访openid
    df_range['is_first_visit'] = df_range['row_num'].map(lambda x: '1' if x == 1.0 else '2')

    # 新访openid
    df_range['is_new_visit'] = df_range['distinct_id'].map(openid_parse)

    # 时间戳格式转换
    df_range['creation_time'] = df_range['creation_time'].map(
        lambda x: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(x)))

    itemfilter = df_range.columns.to_list()
    df_range = df_range.values.tolist()

    # diff ck
    # check channel1,channel2,channel3,channel4,channel5,is_first_visit,is_new_visit

    workers = 2
    lock = Manager().Lock()
    # 'distinct_id','permanentid','creation_time','url','data_date'

    with futures.ProcessPoolExecutor(workers) as executor:
        for item in df_range:
            future = executor.submit(task, item, itemfilter, lock)
            future.add_done_callback(task_after)

