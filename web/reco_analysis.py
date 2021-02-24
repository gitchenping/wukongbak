#encoding=utf-8
import json
from utils.recommend_map import *
from utils import util,log
from .sql import get_filters_where_for_reco,get_sql_data_reco

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
indicator_cal_map={
    '商品曝光pv':"COUNT(CASE WHEN model_type = 1 THEN udid ELSE null END) AS product_expose_pv",
    '商品曝光uv':"COUNT(DISTINCT CASE WHEN model_type = 1 THEN udid ELSE NULL END) AS product_expose_uv",
    '商品点击pv':"COUNT(CASE WHEN model_type = 3 THEN udid ELSE NULL END) AS product_click_pv",
    '商品点击uv':"COUNT(DISTINCT CASE WHEN model_type = 3 THEN udid ELSE NULL END) AS product_click_uv",
    '商品UV点击率':'case when product_expose_uv>0 then round(product_click_uv / product_expose_uv*100,2) else null end as uv_ctr',
    '商品PV点击率':'case when product_expose_pv>0 then round(product_click_pv / product_expose_pv*100,2) else null end as pv_ctr',
    '原始订单行数':'COUNT(CASE WHEN order_id = -1 THEN NULL ELSE order_id END) AS create_hang_num',
    '收订单量':'COUNT(DISTINCT CASE WHEN order_id = -1 THEN NULL ELSE order_id END) AS create_parent_num',
    '收订金额':'SUM(bargin_price * order_quantity) AS create_sale_amt',
    '收订顾客数':'COUNT(DISTINCT CASE WHEN order_id = -1 THEN NULL ELSE order_cust_id END) AS create_cust_num',
    '收订转化率':'case when product_expose_uv>0 then round(create_cust_num / product_expose_uv*100,2) else null end as create_trans_rate',
    '最大曝光位置':'MAX(CASE WHEN model_type = 1 AND position > 0 THEN position ELSE 0 END) AS max_expose_location',
    '平均曝光位置':'ROUND(AVG(CASE WHEN model_type = 1 AND `position` > 0 THEN `position` ELSE 0 END)) AS avg_expose_deepth',
    '最大点击位置':'MAX(CASE WHEN model_type = 3 AND position > 0 THEN position ELSE 0 END) AS max_click_location',
    '平均点击位置':'ROUND(AVG(CASE WHEN model_type = 3 AND `position` > 0 THEN `position` ELSE null END)) AS avg_click_deepth',
    '人均点击次数':'case when product_click_uv>0 then round(product_click_pv / product_click_uv,2) else null end as avg_click_num'
}

#日志设置
reco = log.set_logger('reco.txt')
#推荐conn
conn_ck = util.connect_clickhouse(host='10.7.30.177')

def ck_vs_davi(webdata,filters):
    '''达芬奇数据和ck数据比对'''
    filterdict = {}
    for ele in filters:
        filterdict.update({ele['name']: ele['value'].strip("'")})

    if len(webdata)>0:
        for data in webdata:
            if data['商品ID']=='-1':
                continue
            if data.__contains__('商品PV点击率') and data['商品PV点击率'] is not None :
                data['商品PV点击率']=data['商品PV点击率']*100
            if data.__contains__('商品UV点击率') and data['商品UV点击率'] is not None :
                data['商品UV点击率']=data['商品UV点击率']*100
            if data.__contains__('收订转化率') and data['收订转化率'] is not None :
                data['收订转化率']=data['收订转化率']*100
    else:
        data={}

    sqldata=get_sql_data_reco(data,indicator_cal_map,filterdict,conn_ck)

    davi_data = util.json_format(data, selfdefine='')
    sql_data = util.json_format(sqldata, selfdefine='')

    reco.info('筛选条件：'+str(filterdict))
    util.diff(davi_data,sql_data,reco)


@util.retry(2)
def post(s,token,data):
    headers = {"Content-Type": "application/json", "Authorization": "Bearer " + token}
    searchword_api = "http://newwk.dangdang.com/api/v3/views/72/getdata"
    temp_data = dict(data)
    req = s.post(url=searchword_api, json=temp_data, headers=headers)
    return req

def product_analysis(s,token,requestload):
    '''单品分析'''
    start_date_str = '2021-02-01'
    end_date_str = '2021-02-01'

    for _,platform in platform_name.items():
        for _,shoptype in shop_type_name.items():
            for bd_key,bd_value in bd_name.items():
                pathname_list = path2_name[bd_key]
                for  pathname in pathname_list:

                    for page_key,page_value in page_name.items():
                        module_list = module_name[page_key]
                        for module in module_list:

                            #debug params
                            # platform='全部';shoptype='全部';
                            # bd_value='全部';pathname='全部';
                            # page_value='当当首页';module='feed流'

                            #参数组合
                            params=[
                                    {'name': "start", 'value': "'"+start_date_str+"'"},
                                    {'name': "end", 'value': "'"+end_date_str+"'"},
                                    {'name': "platform_name", 'value':"'"+platform+"'"},
                                    {'name': "shop_type_name", 'value':"'"+shoptype+"'"},
                                    {'name': "bd_name", 'value': "'" + bd_value + "'"},
                                    {'name': "path2_name", 'value': "'" + pathname + "'"},
                                    {'name': "page_name", 'value': "'" + page_value + "'"},
                                    {'name': "model_cn_name", 'value': "'" + module + "'"}
                                    ]

                            filters=[
                                {'name': "平台", 'type': "filter", 'value':"'"+platform+"'", 'sqlType': "STRING", 'operator': "="},
                                {'name': "经营方式", 'type': "filter", 'value':"'"+shoptype+"'", 'sqlType': "STRING",'operator': "="},
                                {'name': "事业部", 'type': "filter", 'value': "'" + bd_value + "'", 'sqlType': "STRING",'operator': "="},
                                {'name': "二级分类", 'type': "filter", 'value': "'" + pathname + "'", 'sqlType': "STRING",'operator': "="},
                                {'name': "页面", 'type': "filter", 'value': "'" + page_value + "'", 'sqlType': "STRING", 'operator': "="},
                                {'name': "模块名称", 'type': "filter", 'value': "'" + module + "'", 'sqlType': "STRING", 'operator': "="}
                            ]

                            requestload['filters']=filters
                            requestload['params']=params

                            print(params)
                            dreq = post(s, token, requestload)
                            if dreq.status_code==200:
                                davi_res=dreq.content
                                full_data=davi_res.decode('utf-8')
                                try:
                                    jsondata=json.loads(full_data)
                                    rawdata=jsondata['payload']['resultList']
                                except Exception as e:
                                    print(e)
                                    print(filters)
                                    continue
                                ck_vs_davi(rawdata,params)




def reco_test():
    #登录
    s,token=util.login_davinci()

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
        "cache": "false", "expired": 300, "flush": "false", "nativeQuery": "false",
        "groups": ["日期",'商品ID','商品名称'],
        'limit':'',
        'nativeQuery':True,
        "orders": [],
        'pageNo':1,
        'pageSize':2
    }

    product_analysis(s,token,requests_load)
