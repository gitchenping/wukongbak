#encoding=utf-8
import json
import requests
from utils import util,log
from utils.report_map import path1_name
from .api import report_api_post
from .sql import report_sql
from logging import config
import logging

log.log_config['handlers']['file']['filename']='logs/report.txt'
config.dictConfig(log.log_config)
report = logging.getLogger('file')

@util.retry(2)
def get_pathlist(level=1,parentid=0):
    api="http://10.4.32.223:8085/api/v3/reportForm/categoryList?level={}&parentId={}".format(level,parentid)
    s = requests.Session()

    req = s.get(url=api)
    apiresult = json.loads(req.content.decode('utf-8'))
    apiresult_list = apiresult['payload']['list']
    apilist=[]
    if apiresult_list != []:
        for ele in apiresult_list:
            apilist.append(ele['path'])
    return apilist

def get_all_path_list():
    categorylist = []
    path1_list=[]
    path2_list=[]
    path3_list=[]

    for path1_key, path1_value in path1_name.items():
        if path1_value == '0':
            categorylist.append('0')
        else:
            categorylist.append(path1_value)
            # print(path1_value)
            path2_list+= get_pathlist(level=2,parentid=path1_value)

    for path2_value in path2_list:
        path3_list += get_pathlist(level=3, parentid=path2_value)[:5]

    return categorylist+path2_list+path3_list

'''
[('0', '', ''), ('1', '', ''), ('1', '1,2', '2,3,7'), ('1', '1', '2'), ('1', '2', '3,7'), ('1', '12,20', '12,20'), 
('1', '20', '20'), ('1', '12', '12'), ('1', '0', '0'), ('1', '3', '26'), ('1', '3', '26'), ('1', '4', '21'), 
('1', '5', '23'), ('1', '7', '25'), ('1', '6', '24'), ('1', '8', '27'), ('1', '9', '28'), ('2', '101,102,103', ''), 
('2', '101', '101'), ('2', '102', '102'), ('2', '103', '103'), ('3', '104', ''), ('4', '105', '')]
'''
def get_all_platform_list(level,parentid):
    api = "http://10.4.32.223:8085/api/v3/reportForm/platformList?level={}&parentId={}".format(level, parentid)
    s = requests.Session()

    req = s.get(url=api)
    apiresult = json.loads(req.content.decode('utf-8'))
    apiresult_list = apiresult['payload']['list']

    platform = []

    if apiresult_list == [] or level >= 4:
        return []

    for ele in apiresult_list:
        source=ele['source']
        if len(source)>1:
            source='2'
        else:
            platform.append((source, ele['platform'], ele['fromPlatform']))
        templist = get_all_platform_list(level + 1, ele['id'])
        platform.extend(templist)

    return platform

category_list=get_all_path_list()
# print(len(category_list))
platform_list=get_all_platform_list(1,'')

def report_product():
    date='2021-02-06'

    for source_platform_fromPlatform in platform_list:      #平台来源
            for bizType in [1,2,3,4,5,6,0]:                     #事业部
                for mgtType in [0,2,1]:                        #经营方式
                    for categorypath in category_list:
                            data={
                                'queryDate': date,
                                'bizType': bizType,
                                'source': source_platform_fromPlatform[0],
                                'platform': source_platform_fromPlatform[1],
                                'fromPlatform': source_platform_fromPlatform[2],
                                'mgtType': mgtType,
                                'categoryPath': categorypath,
                                'startTime': '00:00',
                                'endTime': '12:00'
                            }
                            #debug
                            # data = {
                            #     # 'queryDate': '2021-02-09',
                            #     # 'bizType': 0,
                            #     # 'source': '0',
                            #     # 'platform': '0',
                            #     # 'fromPlatform': '0',
                            #     # 'mgtType': 0,
                            #     # 'categoryPath': '01.00.00.00.00.00',
                            #     # 'startTime': '00:00',
                            #     # 'endTime': '11:00'
                            #
                            # }

                            apidata=report_api_post(data)
                            sqldata=report_sql(data)

                            #diff
                            report.info('筛选条件：' + str(data))
                            util.diff(apidata,sqldata,report)







