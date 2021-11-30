#encoding=utf-8
import json
import random
import math
import requests
from  utils import log,util,db
from  db.map.recommend_map import *
from  db.dao.reco import get_sql_data_reco
from  api.service.reco import reco_api_data

# ck和达芬奇比较
'''
商品名称：
商品曝光PV：
商品曝光UV：指当前查询的模块，其所在楼层或组件下，所展示的商品的曝光UV，使用udid统计UV数据
原始点击PV：
原始点击UV：指当前查询的模块，其所在楼层或组件下，所展示的商品的点击UV，使用udid统计UV数据
商品曝光PV_CTR：商品点击量/商品曝光PV。
商品曝光UV_CTR:商品点击UV/商品曝光UV
原始收订账户数：指当前查询的应用类型，带来的全部收订账户，收订账户根据用户账号id进行统计
原始收订行数：
原始订单行数：指当前查询的模块，带来的订单行数
订单行占比：指当前查询的模块，所产生的原始订单行数占全站订单行数的比例。 公式=原始订单行数/全站订单行数
PV订单转换率：
UV订单转化率：指当前查询的模块，带来的订单行数占推荐商品的点击UV的比例。 公式=原始订单行数/原始点击UV
最大曝光位置：指当前查询的模块，商品的最深曝光位置。依据商品曝光以及坑位顺序统计
平均浏览位置：指当前查询的模块，商品的平均曝光位置
最大点击位置：指当前查询的模块，商品的最深点击位置
平均点击位置：指当前查询的模块，商品的平均点击深度
人均点击位置：商品点击PV/商品点击UV
'''

#日志设置
reco = log.set_logger('reco.txt')
#推荐conn
conn_ck = db.connect_clickhouse(host='10.7.30.177')

def get_ck_data(data):
    '''

    :param filters:
    :return: 返回df
    '''
    filter = dict(data)
    sqldata = get_sql_data_reco(filter,conn_ck)
    return sqldata



def product_analysis(token,requestload):
    '''单品分析'''
    start_date_str = '2021-10-01'
    end_date_str = '2021-10-01'

    for product_id in ['','1454809379'][1:2]:
        for _,platform in platform_name.items():
            for _,shoptype in shop_type_name.items():
                for bd_key,bd_value in bd_name.items():
                    pathname_list = path2_name[bd_key]
                    for  pathname in pathname_list:

                        for page_key,page_value in page_name.items():
                            module_list = module_name[page_key]
                            for module in module_list:

                                #debug params
                                platform='安卓';
                                # shoptype='自营';
                                # bd_value='其他';pathname='全部';
                                # page_value='全部';module='全部'

                                #参数组合
                                params=[
                                        {'name': "start", 'value': "'"+start_date_str+"'"},
                                        {'name': "end", 'value': "'"+end_date_str+"'"},
                                        {'name': "platform", 'value':"'"+platform+"'"},
                                        # {'name': "shop_type_name", 'value':"'"+shoptype+"'"},
                                        {'name': "bd_id", 'value': "'" + bd_value + "'"},
                                        {'name': "path2_name", 'value': "'" + pathname + "'"},
                                        {'name': "page_name", 'value': "'" + page_value + "'"},
                                        {'name': "model_cn_name", 'value': "'" + module + "'"}
                                        ]

                                filters=[
                                    # {'name': "平台", 'type': "filter", 'value':"'"+platform+"'", 'sqlType': "STRING", 'operator': "="},
                                    {'name': "经营方式", 'type': "filter", 'value':"'"+shoptype+"'", 'sqlType': "STRING",'operator': "="},
                                    # {'name': "事业部", 'type': "filter", 'value': "'" + bd_value + "'", 'sqlType': "STRING",'operator': "="},
                                    # {'name': "二级分类", 'type': "filter", 'value': "'" + pathname + "'", 'sqlType': "STRING",'operator': "="},
                                    # {'name': "页面", 'type': "filter", 'value': "'" + page_value + "'", 'sqlType': "STRING", 'operator': "="},
                                    # {'name': "模块名称", 'type': "filter", 'value': "'" + module + "'", 'sqlType': "STRING", 'operator': "="}
                                ]
                                if product_id != '':
                                    prd = {'name': "商品ID", 'type': "filter", 'value': None, 'sqlType': "STRING",
                                     'operator': "="}
                                    prd['value'] ="'"+product_id+"'"
                                    filters.append(prd)

                                requestload['filters']=filters
                                requestload['params']=params

                                #筛选条件
                                filterdict = {}
                                filterdict.update({ele['name']: ele['value'].strip("'") for ele in params})
                                filterdict.update({ele['name']: ele['value'].strip("'") for ele in filters})

                                print(filterdict)
                                #查ck
                                df = get_ck_data(filterdict)

                                dev_total_page = 1
                                pageno = 1
                                pagesize = requestload['pageSize']

                                while pageno <= dev_total_page:

                                    api_data_list,api_data_total_num = reco_api_data(token,requestload)
                                    if pageno == 1:
                                        dev_total_page = math.ceil(api_data_total_num / pagesize)


                                    # 每页随机挑选2个进行测试
                                    length_api_data = len(api_data_list)
                                    choice = random.sample([i for i in range(0, length_api_data)], min(2, length_api_data))

                                    for i in choice:

                                        api_item = api_data_list[i]
                                        if api_item['商品ID'] == '-1':
                                            continue

                                        if api_item.__contains__('商品PV点击率') and api_item['商品PV点击率'] is not None:
                                            api_item['商品PV点击率'] = api_item['商品PV点击率'] * 100
                                        if api_item.__contains__('商品UV点击率') and api_item['商品UV点击率'] is not None:
                                            api_item['商品UV点击率'] = api_item['商品UV点击率'] * 100
                                        if api_item.__contains__('收订转化率') and api_item['收订转化率'] is not None:
                                            api_item['收订转化率'] = api_item['收订转化率'] * 100

                                        api_data = util.data_change(api_item)

                                        # 数据库数据
                                        temp_df = df[df['商品ID'] == api_data['商品ID']]
                                        sql_data = {}
                                        if not temp_df.empty:
                                            sql_item = temp_df.iloc[0].to_dict()
                                            sql_data = util.data_change(sql_item)

                                        #diff
                                        filterdict['商品ID']  = api_data['商品ID']
                                        if sql_data =={} and api_data !={}:
                                            print('test filters no data:'+str(filterdict))
                                            break
                                        diffvalue = util.simplediff(sql_data, api_data)


                                    pageno += dev_total_page // 10 + 1  # 翻页
                                    requestload['pageNo'] = pageno







def reco_test():
    #登录

    token ='eyJhbGciOiJIUzUxMiJ9.eyJ0b2tlbl9jcmVhdGVfdGltZSI6MTYzNDI4MjA0NjgwOCwic3ViIjoiY2hlbnBpbmciL' \
           'CJ0b2tlbl91c2VyX25hbWUiOiJjaGVucGluZyIsImV4cCI6MTYzNDM2ODQ0NiwidG9rZW5fdXNlcl9wYXNzd29yZCI6IkxEQVAifQ.' \
           'o4sNvnK3dN1cBixOwnL2dhWGEmWnAM9K0fV-MjfL51-JwzN__gQFUGDsJKwezsGSWdI6cAJb9BlDuhxsYWOnBg'

    # s,token=util.login_davinci()

    #请求负载
    requests_load={
        "aggregators": [{'column': "商品曝光pv", 'func': "sum"},
                        {'column': "商品曝光uv", 'func': "sum"},
                        {'column': "商品点击pv", 'func': "sum"},
                        {'column': "商品点击uv", 'func': "sum"},
                        {'column': "商品UV点击率", 'func': "sum"},
                        {'column': "商品PV点击率", 'func': "sum"},
                        {'column': "原始订单行数", 'func': "sum"},
                        {'column': "收订单量", 'func': "sum"},
                        {'column': "收订金额", 'func': "sum"},
                        {'column': "收订顾客数", 'func': "sum"},
                        {'column': "收订转化率", 'func': "sum"},
                        {'column': "最大曝光位置", 'func': "sum"},
                        {'column': "平均曝光位置", 'func': "sum"},
                        {'column': "最大点击位置", 'func': "sum"},
                        {'column': "平均点击位置", 'func': "sum"},
                        {'column': "人均点击次数", 'func': "sum"},
                        ],
        "cache": False,
        "expired": 300,
        "flush": False,
        "nativeQuery": False,
        "groups": ["日期",'商品ID','商品名称'],
        'limit':'',
        'nativeQuery':True,
        "orders": [],
        'pageNo':1,
        'pageSize':20,
        'params':None,
        'filters':None

    }

    product_analysis(token,requests_load)

if __name__ == '__main__':

    reco_test()