'''
搜索词分析
'''
import sys,os,requests
import copy
import json,math,random


from db.dao.search_word_analysis import get_sql_data
from utils.util import simplediff
import json


def get_api_data(s,url,data):
    '''

    :param s:
    :param url:
    :param data: 请求参数
    :return:
    '''
    raw_api_result = s.post(url, json = data)
    result_list = []
    api_total_num = 0
    if raw_api_result.status_code == 200:
        json_api_result = json.loads(raw_api_result.content)
        result_list = json_api_result['payload']['resultList']
        api_total_num = json_api_result['payload']['totalCount']

    return result_list,api_total_num

    pass


def do_job(starttime,endtime,s,url):

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
                        {'column': "create_quantity_num", 'func': "sum"},
                        {'column': "no_search_times", 'func': "sum"},
                        {'column': "无结果搜索次数占比", 'func': "sum"},
                        {'column': "search_no_click", 'func': "sum"},
                        {'column': "median_sku_max_ex_location", 'func': "sum"},
                        {'column': "median_sku_max_cl_location", 'func': "sum"},
                        {'column': "avg_sku_max_ex_location", 'func': "sum"},
                        {'column': "avg_sku_max_cl_location", 'func': "sum"},
                        {'column': "跳出率", 'func': "sum"},
                        {'column': "RPM", 'func': "sum"},
                        {'column': "搜索UV价值", 'func': "sum"},
                        {'column': "next_search_word_time", 'func': "sum"},
                        {'column': "zf_prod_sale_amt", 'func': "sum"},
                        {'column': "out_pay_amount", 'func': "sum"}
                        ],
        "cache": "false", "expired": 300, "flush": "false", "nativeQuery": "false",
        "groups": ["date_str", "平台", "搜索来源","search_word"],
        "orders": [{'column': "date_str", 'direction': "desc"}, {'column': "sum(search_times)", 'direction': "desc"}],
        "filters": None,
        "params": [{"name": "start", "value": "'" + starttime + "'"},
                   {"name": "end", "value": "'" + endtime + "'"}
                   ],
        "pageSize": 10,
        "pageNo": 1
    }

    searchword_list = ['', '小王子', '唐诗宋词三百首小学生', '乡土中国'][3:4]
    platform_list = ['安卓','IOS','',]
    searchtype_list = ['','主动搜索','框内搜索词','下拉词引导','热搜榜','历史搜索','搜索发现']

    for searchword in searchword_list:
        for platform in platform_list:
            for searchtype in searchtype_list:

                temp_data = copy.deepcopy(data)
                if searchword != '':
                    temp_data['filters'] = [{'name': "search_word", 'type': "filter", 'value': "'{}'".format(searchword),
                                        'sqlType': "STRING", 'operator': "="}]
                if platform !='':
                    temp_data['params'].append({'name': "platformname", 'value': "'{}'".format(platform)})
                if searchtype !='':
                    temp_data['params'].append({'name': "searchtype", 'value': "'{}'".format(searchtype)})

                #sql data
                filter_where = {ele['name']: ele['value'].strip("'") for ele in temp_data['params'] if ele['name'] not in ['start','end']}
                print(filter_where)
                df = get_sql_data(temp_data)

                #挑选个别页面进行测试
                dev_total_page = 1
                pageno = 1

                while pageno <= dev_total_page:
                    api_result_list,api_result_total_num = get_api_data(s,url,temp_data)

                    length_api_result = len(api_result_list)
                    if pageno == 1:
                        dev_total_page = math.ceil(api_result_total_num / 20)

                    choice = random.sample([i for i in range(0, length_api_result)], min(2, length_api_result))

                    for i in choice:
                        api_data  = {key:value if not isinstance(value,float) else round(value,2) for key,value in api_result_list[i].items() }
                        keyword =  api_data['search_word']
                        date_str = api_data['date_str']


                        sqlresult_match = df[(df['search_word'] == keyword) & (df['date_str'] == date_str)]
                        sqlresult_match = sqlresult_match.where(sqlresult_match.notnull(),None)
                        sql_data  = dict(zip(sqlresult_match.columns, sqlresult_match.values.tolist()[0]))

                        diffvalue = simplediff(sql_data,api_data)
                        filter_where['search_word'] = keyword
                        filter_where['date_str'] = date_str
                        if diffvalue != {}:

                            if os.name == 'posix':
                                search_view_logger.info('筛选条件：'+str(filter_where)+" -Fail")
                                search_view_logger.info(diffvalue)
                                search_view_logger.info('')
                            else:
                                print(filter_where)
                                print(diffvalue)
                        else:
                            search_view_logger.info('筛选条件：' + str(filter_where) + " -Pass")
                    pageno += dev_total_page // 10 + 1  # 翻页
                    temp_data['pageNo'] = pageno


    pass



if __name__ == '__main__':
    starttime = '2021-10-01'
    endtime = '2021-10-02'
    url = "http://newwk.dangdang.com/api/v3/views/375/getdata"

    token = "eyJhbGciOiJIUzUxMiJ9.eyJ0b2tlbl9jcmVhdGVfdGltZSI6MTYzNTk5MzEwMDgxMCwic3ViIjoiY2hlbnBpbmciLCJ0b2tlbl91c2VyX25hbWUiOiJjaGVucGluZyIsImV4cCI6MTYzNjA3OTUwMCwidG9rZW5fdXNlcl9wYXNzd29yZCI6IkxEQVAifQ." \
            "FiRUb377H1vU2nSAuLuSEMT8qlWD4-lO-_lRaG6hzfyRi-GK6J_biRA-zDZAa2nIDKrA_afvt-o-QH1adLETqw"

    s = requests.Session()
    s.headers["Content-Type"] = "application/json"
    s.headers['Authorization'] = "Bearer " + token

    do_job(starttime, endtime, s,url)