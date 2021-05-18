import requests, json
from ._api import post
from utils.util import json_format

'''品类报表api'''
def report_api_category_post(data):
    '''

    :param data: 筛选条件字典
    :return:
    '''
    # headers = {"Content-Type": "application/json", "Authorization": "Bearer " + token}
    report_api = "http://10.4.32.223:8085/api/v3/reportForm/realtime/category"
    # report_api="http://10.4.32.223:8085/api/v3/reportForm/realtime/business"

    temp_data = dict(data)
    apiresult=post(report_api,temp_data,headers={})

    rtnapidata = {}
    if apiresult!=-1:
        apiresult = json.loads(apiresult)
        apiresult_list = apiresult['payload']['modelList']
        apiresult_list_sort = sorted(apiresult_list, key=lambda x: x['path'])

        count = 0
        if len(apiresult_list_sort) > 0:
            # 指标键值对集合
            for ele in apiresult_list_sort:  # 取前两个key-value对
                # key=ele['name']+"_"+ele['path']
                if ele['subsAmount'] != '0.00':
                    key = ele['path']
                    ele.pop('children')
                    ele.pop('name')
                    ele.pop('path')
                    value = json_format(ele, '-')
                    rtnapidata[key] = value
                    count += 1
                if count == 2:
                    break
    return rtnapidata


'''事业部报表api'''
def report_api_bussiness_post(data):
    '''

    :param data: 筛选条件字典
    :return:
    '''
    # headers = {"Content-Type": "application/json", "Authorization": "Bearer " + token}
    report_api = "http://10.4.32.223:8085/api/v3/reportForm/realtime/business"

    temp_data = dict(data)
    apiresult = post(report_api, temp_data, headers={})

    rtnapidata = {}
    if apiresult != -1:
        apiresult=json.loads(apiresult)
        apiresult_list = apiresult['payload']['modelList']

        if len(apiresult_list) > 0:
            # 指标键值对集合
            for ele in apiresult_list:
                if ele['pymtAmount'] != '0.00' and ele['subsAmount'] != '0.00':
                    key = ele['name']
                    ele.pop('name')
                    ele.pop('children')
                    ele.pop('path')
                    value = json_format(ele, '-')
                    rtnapidata[key] = value

    return rtnapidata