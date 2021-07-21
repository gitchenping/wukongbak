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

#mysql
channel_table = {
    'id': '自增id',
    'platform' :'微信小程序',
    #生成规则:p-一级渠道-时间戳 (广告规则 AD ；公众号：GZH；分享裂变：FX)
    'channel_id':'渠道id',
    #指投放前的落地页原路径
    'origin_uri':'渠道链接',
    #投放落地页的链接
    'track_uri' :'投放链接',
    #1:分享裂变 2:小程序广告 3:微信公众号 4:联盟'
    'level_1': '一级渠道' ,
    'level_2':'二级渠道名称',
    'level_3':'三级渠道名称',
    'level_4':'四级渠道名称',
    'level_5':'五级渠道名称',
    'ctime':'创建时间',
    'uptime': '更新时间',
    'click_uptime':'流量更新时间',
    'creator':'创建者',
    #状态 1:正常 -1:删除
    'status':'状态'
}

#logger
report = log.set_logger('applet.txt')

# hive连接
hive_cursor= db.connect_hive()
#ck 连接
#conn_ck = db.connect_clickhouse(host='10.0.5.80')
ck_db={
    'host':"http://10.0.5.80:8123",
    'headers':{'X-ClickHouse-User': 'membersbi', 'X-ClickHouse-Key': 'dangdangbi'}
}
ck_conn=db.CK(ck_db)

#微信小程序渠道明细-日表
dev_mina_table_d = 'dwd.channel_mini_wechat_detail_d'

mina_table_dict = {
    #设备标示-统一字段
    'distinct_id':'',
    #设备标示
    'permanentid':'',
    #创建时间
    'creation_time':'',
    #平台
    'platform':'',
    'channel1':'',
    #二级渠道
    'channel2':'',
    'channel3':'',
    'channel4':'',
    'channel5':'',
    #渠道id
    'union_id':'',
    #是否首访-1为首访2为否
    'is_first_visit':'',
    #是否新访-1为新访2为否
    'is_new_visit':'',
    #访问链接
    'url':'',
    #日期
    'data_date':''
}

lianmeng_data={}
ziran_distinctid_dict={}
openid_new_data = {}
liebian_lanuchs ={"scene":[1007,1008,1048,1047,1155]}
liebian_urls ={
    "/pages/bargain/":"分享裂变|1元砍价|||",
    "/pages/signInAward/":"分享裂变|打卡|||",
    "/pages/zeroYuan/":"分享裂变|0元领|||",
   "/pages/yiFen/":"分享裂变|1分抽奖|||",
    "/pages/bookmark/":"分享裂变|集书签|||",
    "/pages/readingPlan/":"分享裂变|读书计划|||",
    "/pages/shareFreeOrder/":"分享裂变|助力免单|||",
     "/pages/reduceMoney/":"分享裂变|天天领现金|||",
    "/pages/walkEarned/":"分享裂变|步数赚钱|||",
    "/pages/answer/interestingAnswer":"分享裂变|答题领红包|||",
    "/pages/postcard/":"分享裂变|集明信片|||",
    "/pages/readingPlanDaily":"分享裂变|读书计划日常版|||",
    "/pages/walkChallenge":"分享裂变|步数挑战|||",
    "/pages/packetWithdrawal/assistance":"分享裂变|订单红包|||",
    "/pages/signInAward/teamUp":"分享裂变|组队打卡|||",
    "/pages/bookmark/bookmar":"分享裂变|集红包|||",
    "/pages/ddSchool/index":"分享裂变|当当校园|||"
}

unionids={
    "p-324867m-423-101-01":"小程序广告|快友|洽洽瓜子||",
    "p-324867m-423-101-02":"小程序广告|快友|咔吱脆||",
    "p-324867m-423-101-03":"小程序广告|快友|易起抽个奖||",
    "p-324867m-423-101-04":"小程序广告|快友|好券领购||",
    "p-324867m-423-101-05":"小程序广告|快友|会员卡包||",
    "p-324867m-423-101-07":"小程序广告|快友|抽奖助手||",
    "p-324867m-423-101-08":"小程序广告|快友|活动抽奖||",
    "p-324867m-423-101-09":"小程序广告|快友|白象||",
    "p-324867m-423-101-10":"小程序广告|快友|零元砍||",
    "p-324867m-423-101-11":"小程序广告|快友|递易校趣||",
    "p-324867m-423-101-12":"小程序广告|快友|递易校趣||",
    "p-324867m-423-101-13":"小程序广告|快友|怪兽充电||",
    "p-324867m-397-20":"微信公众号|服务号|菜单栏||",
    "p-324867m-397":"微信公众号|服务号|文章||",
    "p-324867m-397-2":"微信公众号|服务号|消息模板||",
    "p-324867m-396":"微信公众号|订阅号|||"
}


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
        allianceid = allianceid.rstrip('m')
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
    #'{"path":"pages/signInAward/index","query":{"share_id":"ebJzlJCAMJzIeCDl","shareDate":"20210704","roundNum":"01",
    # "ald_share_src":"0f44d981855f5ca6933cd7e846ed24d9","behavior":"share"},"scene":1044,"shareTicket":"87bafa08-1bd2-44b2-af20-e4348404f87a","referrerInfo":{},"locationInfo":{"isPrivateMessage":false},"mode":"default"}'
    #
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
    #如果存在union_id
    if unionids.__contains__(union_id):
        channels = unionids[union_id].split('|')
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
    if lianmeng_data.__contains__(allianceid):
        alliancename = lianmeng_data[allianceid]
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

#openid 是否为新用户
def openid_parse(distinctid):
    is_new_visit = '2'
    if openid_new_data.__contains__(distinctid) and distinctid !='':
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


def test_channel_mina_detail(init_start=200000,init_end=300000):
    '''渠道明细'''
    global lianmeng_data
    global ziran_distinctid_dict
    global openid_new_data
    lianmeng_data = {}
    ziran_distinctid_dict = {}
    openid_new_data = {}

    data_date = '2021-07-04'
    cwd = os.getcwd()
    parent = os.path.dirname(cwd)

    miniprogramhour_sql = '''
            select 
                openid as distinct_id,
                permanentid,
                `time` as creation_time,
                url,
                (case when platform='wechat_mina' then '4' else platform end) as platform,
                lanuch_info,
                data_date 
            from 
                ods.miniprogramhour 
            where 
                data_date='{date}' and platform='wechat_mina' and type='1'
            order by distinct_id
        '''

    lianmeng_sql = '''
            select
                allianceid,
                alliancename
            from
                ods.alliancemember
    '''

    #新访openid
    mini_view_new_sql ='''
            select 
                openid,1 as new 
            from 
                dwd.flow_mini_view_new 
            where data_date='{date}' and platform='wechat_mina'
    '''


    if not os.path.exists("logs/mini_base.pickle"):
        hive_cursor.execute(miniprogramhour_sql.format(date=data_date))
        base_data = hive_cursor.fetchall()
        with open('logs/mini_base.pickle', 'wb') as f:
            pickle.dump(base_data, f)
    else:
        with open('logs/mini_base.pickle', 'rb') as f:
            base_data = pickle.load(f)


    columns = ['distinct_id','permanentid','creation_time','url','platform','lanuch_info','data_date']

    df = pd.DataFrame(base_data,columns = columns)

    #数据量太大截断处理
    distinctid_list = df['distinct_id'].to_list()
    record_num = len(distinctid_list)

    if record_num <= init_start:
        endline_num = record_num
        startline_num = 0
    elif record_num> init_start and record_num <= init_end:

        endline_num = record_num
        startline_num = init_start
    else:
        startline_num = init_start
        endline_num = init_end

    #截取完整的distinct_id
    id = distinctid_list[startline_num]
    i= startline_num - 1
    while i>=0 and distinctid_list[i] == id:
        i-=1
    startline_num = i+1


    id = distinctid_list[endline_num]
    i = endline_num + 1

    while i< record_num and distinctid_list[i] == id:
        i+=1
    endline_num = i

    df_2 = df[startline_num:endline_num]

    #
    unionid_allianceid_series = df_2['url'].map(unionid_allianceid_parse)

    #添加两列
    df_2[['union_id','allianceid']] = unionid_allianceid_series.apply(pd.Series)

    #渠道
    temp_channel = df_2[['url','lanuch_info','union_id']].apply(channel_parse,axis=1)
    #增加渠道列
    df_2[['channel1', 'channel2','channel3','channel4','channel5']] = temp_channel.apply(pd.Series)

    df_2 = df_2.drop(['lanuch_info'],axis=1)        #舍弃该列

    #联盟账户字典
    if not os.path.exists("logs/mini_lianmeng.pickle"):
        hive_cursor.execute(lianmeng_sql)
        lianmeng_data = hive_cursor.fetchall()
        lianmeng_data = dict(lianmeng_data)
        with open('logs/mini_lianmeng.pickle', 'wb') as f:
            pickle.dump(lianmeng_data, f)
    else:
        with open('logs/mini_lianmeng.pickle', 'rb') as f:
            lianmeng_data = pickle.load(f)

    temp_channel = df_2[['channel1', 'channel2','allianceid']].apply(lianmeng_channel, axis=1)
    df_2[['channel1', 'channel2']] = temp_channel.apply(pd.Series)

    #自然渠道的用户
    dfgroupby = df_2[['distinct_id','channel1']].groupby('distinct_id')
    for id,channel_df in dfgroupby:
        # 自然流量用户 （一级渠道为null 且只有一个一级渠道）
        temp = channel_df['channel1']
        if temp.nunique() == 1 and temp.unique()[0] == 'null':
            ziran_distinctid_dict[id] = 1

    #自然量标识
    df_2['channel1'] = df_2[['distinct_id', 'channel1']].apply(ziran_parse,axis=1)

    #排名
    df_2['creation_time'] = df_2['creation_time'].astype('int')
    df_2['row_num'] = df_2['creation_time'].groupby(df['distinct_id']).rank(method='first')      #分组排序

    #首访openid
    df_2['is_first_visit']  = df_2['row_num'].map(lambda x:'1' if x==1.0 else '2')

    #新访openid
    if not os.path.exists("logs/mini_view_new.pickle"):
        hive_cursor.execute(mini_view_new_sql.format(date=data_date))
        openid_new_data = hive_cursor.fetchall()
        openid_new_data = dict(openid_new_data)
        with open('logs/mini_view_new.pickle', 'wb') as f:
            pickle.dump(openid_new_data, f)
    else:
        with open('logs/mini_view_new.pickle', 'rb') as f:
            openid_new_data = pickle.load(f)
    df_2['is_new_visit'] = df_2['distinct_id'].map(openid_parse)

    #时间戳格式转换

    df_2['creation_time'] = df_2['creation_time'].map(lambda x: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(x/1000)))

    itemfilter = df_2.columns.to_list()
    df_2 = df_2.values.tolist()

    #diff ck
    #check channel1,channel2,channel3,channel4,channel5,is_first_visit,is_new_visit

    workers = 2
    lock = Manager().Lock()
    # 'distinct_id','permanentid','creation_time','url','data_date'

    with futures.ProcessPoolExecutor(workers) as executor:
        for item in df_2:
            future = executor.submit(task, item,itemfilter,lock)
            future.add_done_callback(task_after)



