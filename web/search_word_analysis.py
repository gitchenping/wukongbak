#encoding=utf-8
from utils import util
from utils.decorate import complog
import math

#口径定义
'''
搜索次数（sum(search_times)）：关键词搜索事件条数（事件id）
点击次数（sum(click_times)）：搜索结果页商品点击事件或加车事件的条数
搜索UV（sum(search_uv)）：关键词搜索事件UUID去重用户数 即为搜索UV
点击UV（sum(click_uv)）：搜索结果页商品点击事件或加车事件UUID去重用户数 
搜索UV点击率（sum(搜索UV点击率)）：点击UV/搜索UV

收订用户数（sum(create_cust_num)）：当天内用户对搜索商品点击、加购，且在当天内对此商品进行下单
收订用户转化率（sum(收订用户转化率)）：收订人数/搜索UV
收订单量（sum(create_parent_num)）：
收订金额（sum(create_sale_amt)）：

无结果搜索次数（sum(no_search_times)）：发起搜索请求且请求结果为0的次数
无结果搜索次数占比（sum(无结果搜索次数占比)）：
搜索无点击次数（sum(search_no_click)）：发起搜索请求且无点击行为的次数
平均商品曝光最大位置（sum(avg_sku_max_ex_location)）：每次请求曝光商品最大位置
平均商品点击最大位置（sum(avg_sku_max_cl_location)）：
RPM（sum(RPM)）：搜索收订金额/搜索次数*1000
搜索UV价值（sum(搜索UV价值)）：搜索收订金额/搜索UV

'''

#日志logger
report=log.set_logger('searchword.txt')

def search_top1000(s,token,data):
    '''top1000分析'''
    headers = {"Content-Type": "application/json", "Authorization": "Bearer " + token}
    searchword_api = "http://newwk.dangdang.com/api/v3/views/31/getdata"
    temp_data = dict(data)
    print('now runing ' + str(data))

    req = s.post(url=searchword_api, json=temp_data, headers=headers)
    try:
        payload = util.get_search_word_api_content(req.content)
        apiresult_num = int(payload['totalCount'])

        apidata = payload['resultList']
    except Exception:
        apiresult_num = 0

    final_api_result = {}
    if apiresult_num > 0 and len(apidata) > 0:
        final_api_result = {ele['搜索词']: ele['sum(搜索次数)'] for ele in apidata}

    # 数据库
    final_sql_result = filters.get_search_top1000_sql_content(data)
    # api和sql比对
    util.diff_search(final_api_result, final_sql_result)

@util.retry(2)
def post(s,token,data):
    headers = {"Content-Type": "application/json", "Authorization": "Bearer " + token}
    searchword_api = "http://newwk.dangdang.com/api/v3/views/30/getdata"
    temp_data = dict(data)
    req = s.post(url=searchword_api, json=temp_data, headers=headers)
    return req

def search_word(s,token,start_date,end_date):
    platformdict={'全部':'1'}
    # platformdict = {'安卓':'3','IOS':'2'}
    #['9.0$cju1cmhizvi₰打кǎit~bao或點缶url链 https://m.tb.cn/h.4ejd06y?sm=227403']
    for keyword in ['']:

        if keyword!='':
            filters_value=[{'name': "search_word", 'type': "filter", 'value': "'"+keyword+"'", 'sqlType': "STRING", 'operator': "="}]
        else:
            filters_value=[]

        for platform in platformdict.keys():

            if platform!='全部':
                params_value=[{'name': "platformname", 'value': "'"+platform+"'"}]
            else:
                params_value=[]
            data = {
                "aggregators": [{'column': "search_times", 'func': "sum"},
                                {'column': "search_uv", 'func': "sum"},
                                {'column': "click_times", 'func': "sum"},
                                {'column': "click_uv", 'func': "sum"},
                                {'column': "搜索UV点击率", 'func': "sum"},
                                {'column': "create_cust_num", 'func': "sum"},
                                {'column': "收订用户转化率", 'func': "sum"},
                                {'column': "create_parent_num", 'func': "sum"},
                                {'column': "create_sale_amt", 'func': "sum"},
                                {'column': "no_search_times", 'func': "sum"},
                                {'column': "无结果搜索次数占比", 'func': "sum"},
                                {'column': "search_no_click", 'func': "sum"},
                                {'column': "avg_sku_max_ex_location", 'func': "sum"},
                                {'column': "avg_sku_max_cl_location", 'func': "sum"},
                                {'column': "RPM", 'func': "sum"},
                                {'column': "搜索UV价值", 'func': "sum"}
                                ],
                "cache": "false", "expired": 300, "flush": "false", "nativeQuery": "false",
                "groups": ["date_str", "平台", "search_word"],
                "orders": [{'column': "date_str", 'direction': "desc"},{'column': "sum(search_times)", 'direction': "desc"}],
                "filters": filters_value,
                "params": [{"name": "start", "value": "'" + start_date + "'"},
                           {"name": "end", "value": "'" + end_date + "'"}
                           ]+params_value,
                "pageSize": 10,
                "pageNo": 380
            }

            data_top1000 = {
                "aggregators": [{'column': "搜索次数", 'func': "sum"}
                                ],
                "cache": "false", "expired": 300, "flush": "false", "nativeQuery": "false",
                "groups": ["搜索词"],
                "orders": [],
                "filters": filters_value,
                "params": [{"name": "start", "value": "'" + start_date + "'"},
                           {"name": "end", "value": "'" + end_date + "'"}
                           ] + params_value

            }
            # search_top1000(s,token,data_top1000)
            searchword(s,token,data)

@complog(report)
def searchword(s,token,data):
    '''搜索词分析'''
    # headers = {"Content-Type": "application/json", "Authorization": "Bearer " + token}
    # searchword_api = "http://newwk.dangdang.com/api/v3/views/30/getdata"
    # temp_data = dict(data)
    # print('now runing ' + str(data))

    search_times=100     #默认api请求查询次数

    i=data['pageNo']
    start_page=i
    success_=0
    fail_=0
    while i<=search_times+start_page:
        # req = s.post(url=searchword_api, json=temp_data, headers=headers)
        req=post(s,token,data)
        try:
            payload = util.get_search_word_api_content(req.content)
            apiresult_num = int(payload['totalCount'])

            apidata=payload['resultList']
        except Exception:
            apiresult_num=0

        final_api_result={}
        page_num=0
        if apiresult_num > 0 and len(apidata)>0:
            page_num = math.ceil(apiresult_num / 20)

            #数据进行处理-以日期_平台_搜索词为键，其他值为value
            for item in apidata:

                # key=item.pop('date_str')+"_"+item.pop('平台')+"_"+item.pop('search_word')

                for _key,_value in item.items():
                    item[_key]= util.format_precision(_value)
                #传item去数据库查询
                sql_item=filters.get_search_word_sql_content_2(item)
                util.diff_search(item, sql_item)
        #         final_api_result[key]=item
        #
        #
        #     #数据库
        #     final_sql_result=filters.get_search_word_sql_content(i-1,data)
        #
        # #api和sql比对
        # util.diff_search(final_api_result,final_sql_result)

        i+=1
        # temp_data['pageNo'] =i
        data['pageNo'] = i
        if i>page_num:
            break
    print("run "+str(apiresult_num)+" items ,success "+str(success_)+" items ,fail "+str(fail_)+" items")

