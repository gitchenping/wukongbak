'''
#实时搜索词-接口校验
'''
import os
import sys
import random
import pandas as pd
import numpy as np

sys.path.append("..")
cwd = os.getcwd()
parent = os.path.dirname(cwd)
father_path = os.path.abspath(parent + os.path.sep + "..")
sys.path.append(father_path)

import json,math
import requests,datetime
from itertools import combinations, chain
from utils.db import CK
from utils.log import get_logger
from utils.util import diff


ck_table = "bi_mdata.realtime_search_word_all"

ck_db = {
    'host': "http://10.0.5.80:8123",
    'headers': {'X-ClickHouse-User': 'membersbi', 'X-ClickHouse-Key': 'dangdangbi'}
}

ck_conn = CK(ck_db)
real_search_logger = get_logger(filename = 'realtime_search_word')

main_table = {
    'clickPv': '点击PV',
    'clickPvLinkRelative': '点击PV环比',
    'clickPvWoW': '点击PV同比上周',
    'clickPvYoY': '点击PV同比去年',
    'clickUv': '点击UV',
    'clickUvLinkRelative': '点击UV环比',
    'clickUvWoW': '点击UV同比上周',
    'clickUvYoY': '点击UV同比去年',
    'createCustNum': '收订用户数',
    'createCustNumLinkRelative': '收订用户数环比',
    'createCustNumWoW': '收订用户数同比上周',
    'createCustNumYoY': '收订用户数同比去年',
    'createParentNum': '收订订单数',
    'createParentNumLinkRelative': '收订订单数环比',
    'createParentNumWoW': '收订订单数同比上周',
    'createParentNumYoY': '收订订单数同比去年',
    'createSaleAmt': '收订金额',
    'createSaleAmtLinkRelative': '收订金额环比',
    'createSaleAmtWoW': '收订金额同比上周',
    'createSaleAmtYoY': '收订金额同比去年',
    'mainProductId': '主品ID',

    'path2Name': '二级类',
    'rpm': 'RPM',
    'rpmLinkRelative': 'RPM环比',
    'rpmWoW': 'RPM同比上周',
    'rpmYoY': 'RPM同比去年',
    'searchAmtRate': '搜索UV价值',
    'searchAmtRateLinkRelative': '搜索UV价值环比',
    'searchAmtRateWoW': '搜索UV价值同比上周',
    'searchAmtRateYoY': '搜索UV价值同比去年',
    'searchClickRate': '搜索UV点击率',
    'searchCustRate': '收订用户转化率',
    'searchPv': '搜索PV',
    'searchPvLinkRelative': '搜索PV环比',
    'searchPvWoW': '搜索PV同比上周',
    'searchPvYoY': '搜索PV同比去年',
    'searchUv': '搜索UV',
    'searchUvLinkRelative': '搜索UV环比',
    'searchUvWoW': '搜索UV同比上周',
    'searchUvYoY': '搜索UV同比去年',
    'searchWord': '搜索词',

}

sql_table = {
    'search_word': '搜索词',
    'path2_name': '二级分类名称',
    'main_product_id': '主品ID',
    'search_uv': '搜索uv',
    'search_pv': '发起搜索请求的次数',
    'click_uv': '点击uv',
    'click_pv': '点击次数',
    'create_cust_num': '收订用户数',
    'create_parent_num': '收订订单数',
    'create_sale_amt': '收订金额'
}

anomaly = {
    # '':'全部',
    "1": '仅异动'
}

comparetype = {
    1: 'YoY',
    2: 'WoW',
    3: 'LinkRelative',
}

word_select_time = {
    '1': ">=1",
    '2': ">=2",
    '3': ">=3",
    '4': ">=4",
    '5': ">=5",
    '6': ">=6",
    '7': ">=7",
    '8': ">=8",
    '9': ">=9",
    '10': ">=10",
}


def data_change(data):
    '''
    格式转换
    :param data:
    :return:
    '''
    temp = dict(data)
    for key, value in temp.items():
        if value is None or value == 'inf':
            temp[key] = '-'
        elif isinstance(value, str):
            try:
                value_new = round(eval(value.strip('%')), 2)
            except Exception:
                value_new = value
            temp[key] = value_new
        elif isinstance(value, float):
            temp[key] = round(value, 2)
        else:
            continue
    return temp


def get_api_data(url, data, s):
    '''

    :param data:
    :return:
    '''

    searchword_api = url
    temp_data = dict(data)
    req = s.post(url=searchword_api, json=temp_data)

    apiresult_list = []
    total_num = 0

    req_status_code = req.status_code

    if req_status_code == 200:
        apiresult_raw = json.loads(req.content.decode('utf-8'))
        # print(apiresult_raw)
        if url.endswith('searchwordLine'):
            apiresult_list = apiresult_raw['payload']
            total_num = None
        else:
            apiresult_list = apiresult_raw['payload']['modelList']  # 无返回数据 apiresult_list =[]
            total_num = int(apiresult_raw['payload']['total'])
    elif req_status_code == 403:
        print('token invalid')
        sys.exit(0)
        # print(total_num)
    return apiresult_list, total_num

    pass


def get_sql_data(data, date_list, ciyun=False, wordline=False, ck=None):
    '''
    查数据库
    :param data:
    :return:
    '''
    tempdata = dict(data)

    searchword = tempdata['searchword']
    if searchword == '':
        searchword_where = "1=1"
    else:
        searchword_where = "search_word='{searchword}'".format(searchword=searchword)

    sql_date_list = str(tuple(date_list))

    where = " where "
    date_str = date_list[0]

    if not wordline:
        hour_str = "'{hour_str}'"
        hour_str_value = str(int(tempdata['endTime'].split(":")[0]))
        if len(hour_str_value) == 2:
            word_select_where = word_select_time['10']
        else:
            word_select_where = word_select_time[hour_str_value]

        where += "search_pv " + word_select_where + " and main_product_id <> -1 and "

    else:
        hour_str = "{hour_str}"
        hour_str_value = "(SELECT toString(max(toUInt32(hour_str))) FROM bi_mdata.realtime_search_word_all where date_str ='{date_str}')"

    where += " {searchword_where} and date_str in {sql_date_list} and hour_str = " + hour_str
    where_format = where.format(searchword_where=searchword_where, sql_date_list=sql_date_list, hour_str=hour_str_value)

    select_column = ','.join([key for key in sql_table.keys()])
    groupby = ""
    orderby = ""

    select_column_list = [''.join([key.capitalize() for key in key.split('_')]) for key in sql_table.keys()]
    item_key_list = [a[0].lower() + a[1:] for a in select_column_list] + ['date_str']

    if ciyun:
        orderby_item = data['orderBy']
        click_search_lower = orderby_item[:-2].lower()
        uv_pv_lower = orderby_item[-2:].lower()

        orderby_item_sql = click_search_lower + "_" + uv_pv_lower
        orderby = " order by " + orderby_item_sql + " desc "
        if data['anomaly'] == '':
            orderby += " limit 50"

    if wordline:
        item_key_list = [key for key in sql_table.keys()]
        select_column = ','.join(item_key_list)
        item_key_list.append('date_str')

        if tempdata['anomaly'] != '1':

            if tempdata['searchword'] == '':
                item_key_list = ['search_uv', 'click_uv', 'search_pv', 'click_pv', 'create_cust_num']
                select_column = ','.join(["sum(" + ele + ")" for ele in item_key_list])
                item_key_list.append('date_str')
                groupby = " group by date_str "

    sql = "select " + select_column + ",date_str from " + ck_table + where_format.format(
        date_str=date_str) + groupby + orderby

    sql_result = ck_conn.ck_get(sql)
    # 转为df
    df = pd.DataFrame(sql_result, columns=item_key_list)

    if data['anomaly'] == '1':  # 异动处理
        # 查询search_pv 环比大于等于0.5的同比
        pre_date_str = get_interval_day(date_list[0], 1).strftime('%Y-%m-%d')

        now_where = where.format(searchword_where=searchword_where, sql_date_list="('" + date_list[0] + "')",
                                 hour_str=hour_str_value.format(date_str=date_str))

        pre_where = where.format(searchword_where=searchword_where, sql_date_list="('" + pre_date_str + "')",
                                 hour_str=hour_str_value.format(date_str=pre_date_str))

        sql = " select t1.search_word,t1.date_str from (select search_word,search_pv,date_str from " + ck_table + now_where + ") t1 " \
                                                                                                                              " inner join (select search_word,search_pv from " + ck_table + pre_where + ") t2 on " \
                                                                                                                                                                                                         "t1.search_word = t2.search_word where t2.search_pv is not null and t2.search_pv<>0 and round(t1.search_pv / t2.search_pv,2) >=1.50"

        sql_result = ck_conn.ck_get(sql)
        anomal_dict = dict(sql_result)

        if wordline:
            search_key = "search_word"
        else:
            search_key = "searchWord"

        df = df[df[search_key].isin(anomal_dict)]
        if ciyun:
            df = df.iloc[0:50]
        if wordline:
            df = df.apply(pd.to_numeric, errors='ignore')
            df = df.groupby('date_str').sum()

    return df


def get_comparetype_list():
    '''
    同比环比组合
    :return:
    '''
    typelist = []
    for key, value in comparetype.items():
        typelist.append(key)
    result = [list(combinations(typelist, i)) for i in range(0, 4)]
    return [str(list(ele)) for ele in chain.from_iterable(result)]


def get_all_filters(date, starttime, endtime):
    all_filtes_list = []

    searchword_list = ['', '辛亥革命', '诺贝尔文学奖作品', '乡土中国'][0:1]
    querydate = date
    compretype_list = get_comparetype_list()

    for searchword in searchword_list:
        for anomal in anomaly.keys():
            for comparetype in compretype_list:
                all_filtes_list.append(
                    {
                        'searchword': searchword,
                        'anomaly': anomal,
                        'compareType': comparetype,
                        'queryDate': date,
                        'startTime': starttime,
                        'endTime': endtime,
                        'pageNo': 1,
                        'pageSize': 10,
                        'sort': '',
                        'orderBy': ''

                    }
                )
    # debug
    # debug_params = {}
    # all_filtes_list.insert(0, debug_params)

    return all_filtes_list


def do_job(date, starttime, endtime, s):
    url = "http://test.newwk.dangdang.com/api/v3/reportForm/realtime/searchword"
    # 遍历前端筛选条件
    filter_list = get_all_filters(date, starttime, endtime)

    len1 = len(filter_list)
    print(len1)

    # 有同环比的指标
    key_tbhb = set()
    tbhb_dict = {}
    for key in main_table.keys():
        if key.endswith('WoW'):
            key_tbhb.add(key[:-3])
        elif key.endswith('YoY'):
            key_tbhb.add(key[:-3])
        elif key.endswith('LinkRelative'):
            key_tbhb.add(key[:-12])
        else:
            continue
        tbhb_dict[key] = '-'  # 赋初值

    for filter in filter_list[0:]:
        # 获取api结果
        data = dict(filter)

        # 查数据库
        compartype_list = eval(data['compareType'])

        date_str = data['queryDate']
        hour_str = data['endTime'].split(":")[0]
        date_list = [date_str]

        for i in compartype_list:
            tbhb_endstr = comparetype[i]
            if tbhb_endstr == 'YoY':
                tempdate = get_last_year_date(date_str)
            elif tbhb_endstr == 'WoW':
                tempdate = get_interval_day(date_str, 7).strftime('%Y-%m-%d')
            else:
                tempdate = get_interval_day(date_str, 1).strftime('%Y-%m-%d')

            date_list.append(tempdate)

        df = get_sql_data(data, date_list)

        dev_total_page = 1
        pageno = 1

        while pageno <= dev_total_page:

            api_data_list, api_data_total_num = get_api_data(url, data, s)
            if pageno == 1:
                dev_total_page = math.ceil(api_data_total_num / 10)

            length_api_data = len(api_data_list)
            if df.empty:
                if length_api_data != 0:
                    real_search_logger.info('数据库无此类数据，' + " 筛选条件:" + str(data))
                break

            # 每页随机挑选2个进行测试
            choice = random.sample([i for i in range(0, length_api_data)], min(2, length_api_data))

            for i in choice:
                tbhb_dict_copy = dict(tbhb_dict)

                item = api_data_list[i]
                keyset = set(main_table) & set(item)

                api_item = {key: value for key, value in item.items() if key in keyset}

                apiresult = {key: '-' if data['compareType'] == '[]' and key.endswith('Relative') else value for
                             key, value in api_item.items()}
                apiresult = data_change(apiresult)
                apiresult['date_str'] = date_str
                apiresult['hour_str'] = hour_str

                sqlresult = {}
                searchWord = api_item['searchWord']
                sqlresult_match = df[df['searchWord'] == searchWord]

                # 计算同环比
                row_index = sqlresult_match.index.tolist()
                if len(row_index) > 0:

                    if date_str in sqlresult_match['date_str'].values:  # 可以进行同环比计算
                        sqlresult_match.sort_values('date_str', axis=0, ascending=False, inplace=True)
                        sqlresult_match = sqlresult_match.apply(pd.to_numeric, errors='ignore')

                        sqlresult_match['searchClickRate'] = round(
                            sqlresult_match['clickUv'] / sqlresult_match['searchUv'], 2)
                        sqlresult_match['searchCustRate'] = round(
                            sqlresult_match['createCustNum'] / sqlresult_match['searchUv'], 2)
                        sqlresult_match['searchAmtRate'] = round(
                            sqlresult_match['createSaleAmt'] / sqlresult_match['searchUv'], 2)
                        sqlresult_match['rpm'] = round(
                            sqlresult_match['createSaleAmt'] / sqlresult_match['searchPv'] * 1000, 2)

                        sqlresult_match.replace(np.inf, np.nan, inplace=True)
                        # sqlresult_match = sqlresult_match.where(sqlresult_match.notnull(), None)

                        # 同环比
                        tbhb_key_list = list(key_tbhb)
                        tbhb_date_list = sqlresult_match['date_str'].to_list()
                        tbhb_df = sqlresult_match[key_tbhb]

                        base_date = datetime.strptime(tbhb_date_list[0], '%Y-%m-%d')

                        for i in range(1, len(tbhb_date_list)):
                            tbhb_date_date = datetime.strptime(tbhb_date_list[i], '%Y-%m-%d')

                            diffdate = base_date - tbhb_date_date

                            temp_df = round((tbhb_df.iloc[0] / tbhb_df.iloc[i] - 1) * 100, 2)
                            temp_df.replace(np.inf, np.nan, inplace=True)
                            temp_df = temp_df.where(temp_df.notnull(), '-')
                            if diffdate.days == 1:
                                rwy = [ele + "LinkRelative" for ele in tbhb_key_list]

                            elif diffdate.days == 7:
                                rwy = [ele + "WoW" for ele in tbhb_key_list]

                            else:
                                rwy = [ele + "YoY" for ele in tbhb_key_list]

                            tbhb_dict_copy.update(dict(zip(rwy, temp_df)))
                        # 合并值
                        sqlresult = sqlresult_match.iloc[0].to_dict()
                        sqlresult = data_change(sqlresult)
                        sqlresult.update(tbhb_dict_copy)
                        sqlresult['hour_str'] = hour_str

                # diff
                message = '-Success-'
                diffvalue = ''

                diff_value = diff(sqlresult, apiresult)

                filter_output = dict(data)
                filter_output['searchWord'] = searchWord
                print(filter_output)
                if diff_value != {}:
                    message = '-Fail-'
                    diffvalue = diff_value

                    real_search_logger.info(message + " 筛选条件:" + str(filter_output))
                    real_search_logger.info(str(diffvalue))
                    real_search_logger.info('\n')

            pageno += dev_total_page // 10 + 1  # 翻页
            data['pageNo'] = pageno

    pass


def do_ciyun_job(date, starttime, endtime, s):
    url = "http://test.newwk.dangdang.com/api/v3/reportForm/realtime/searchwordCiYun"

    # 遍历前端筛选条件
    filter_list = get_all_filters(date, starttime, endtime)

    len1 = len(filter_list)
    print(len1)

    for filter in filter_list:
        data = dict(filter)

        date_str = data['queryDate']
        date_list = [date_str]
        data['sort'] = 'desc'
        data['pageSize'] = 50
        for item in ['searchPv', 'searchUv', 'clickPv', 'clickUv']:

            data['orderBy'] = item
            print(data)
            api_data_list, api_data_total_num = get_api_data(url, data, s)

            api_ciyun = set(ele['searchWord'] for ele in api_data_list)

            df = get_sql_data(data, date_list=date_list, ciyun=True, wordline=False)

            sql_ciyun = set(df['searchWord'].values.tolist())

            diffciyun_sql_api = sql_ciyun - api_ciyun
            diffciyun_api_sql = api_ciyun - sql_ciyun
            message = ""
            if diffciyun_sql_api != set():
                message += 'test has words:' + str(diffciyun_sql_api) + ",dev hasn't."
            if diffciyun_api_sql != set():
                message += 'dev has words:' + str(diffciyun_api_sql) + ",test hasn't"
            if message != '':
                real_search_logger.info(" 筛选条件:" + str(data))
                real_search_logger.info(message)
                real_search_logger.info('\n')

    pass


def do_wordline_job(date, starttime, endtime, s):
    url = "http://test.newwk.dangdang.com/api/v3/reportForm/realtime/searchwordLine"

    findby_list = ['search_pv', 'search_uv', 'click_pv', 'click_uv', 'search_click_rate', 'search_cust_rate']

    date_list = [datetime.strftime(datetime.strptime(date, '%Y-%m-%d') - timedelta(days=i), '%Y-%m-%d') for i in
                 range(8)]
    # 遍历前端筛选条件
    filter_list = get_all_filters(date, starttime, endtime)

    len1 = len(filter_list)
    print(len1)

    #
    all_com = [list(combinations(findby_list, i)) for i in range(1, 5)]
    findby_iter = list(chain.from_iterable(all_com))

    for filter in filter_list:
        data = dict(filter)

        date_str = data['queryDate']

        for item in findby_iter[0:1]:
            data['findBy'] = str(list(item))

            # api
            api_data_dict, api_data_total_num = get_api_data(url, data, s)

            for key, value in api_data_dict.items():
                if isinstance(value, list):
                    api_data_dict[key] = {key: value for ele in value for key, value in ele.items()}

            # clickhouse
            df = pd.DataFrame([], columns=[])
            data_list_zip = []
            for each_date in date_list:
                temp_df = get_sql_data(data, [each_date], ciyun=False, wordline=True)
                if not temp_df.empty:
                    data_list_zip.append(each_date)
                    df = df.append(temp_df, ignore_index=True)

            df = df.apply(pd.to_numeric, errors='ignore')
            df['search_click_rate'] = round(df['click_uv'] / df['search_uv'], 2)
            df['search_cust_rate'] = round(df['create_cust_num'] / df['search_uv'], 2)

            sql_data_dict = {}

            for ele in findby_list:
                value = df[ele].values.tolist()
                sql_data_dict[ele] = dict(zip(data_list_zip, value))

            # diff
            message = '-Success-'
            diffvalue = ''

            diff_value = diff(sql_data_dict, api_data_dict)

            filter_output = dict(data)
            print(filter_output)
            if diff_value != {}:
                message = '-Fail-'
                diffvalue = diff_value

                real_search_logger.info(message + " 筛选条件:" + str(filter_output))
                real_search_logger.info(str(diffvalue))
                real_search_logger.info('\n')

    pass


if __name__ == '__main__':

    a = ''
    if a == '':
        a = datetime.now()
        date = datetime.strftime(a, "%Y-%m-%d")
        now_hour = a.hour
    else:
        date = a
        now_hour = 12

    starttime = '00:00'

    if now_hour < 10:
        endtime = "0" + str(now_hour) + ":00"
    else:
        endtime = str(now_hour) + ":00"

    token = "eyJhbGciOiJIUzUxMiJ9.eyJ0b2tlbl9jcmVhdGVfdGltZSI6MTYzNDExMDI3NTI4OSwic3ViIjoiY2hlbnBpbmciLCJ0" \
            "b2tlbl91c2VyX25hbWUiOiJjaGVucGluZyIsImV4cCI6MTYzNDE5NjY3NSwidG9rZW5fdXNlcl9wYXNzd29yZCI6IkxEQVAifQ." \
            "dnVgEnxYIJZDa12AHo-GMjGheD1-oUz-eWscyO9Ux-bWtnsmXAgAgsj0sIqbQGWfKCtBx9ugv8TL5h2WhgKBbg"

    s = requests.Session()
    s.headers["Content-Type"] = "application/json"
    s.headers['Authorization'] = "Bearer " + token

    #
    # do_job(date,starttime,endtime,s)
    #
    # do_ciyun_job(date,starttime,endtime,s)
    # #
    do_wordline_job(date, starttime, endtime, s)