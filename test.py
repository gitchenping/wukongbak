import requests
import json
from utils import util,_sql
from web import sql,api

def post(data):
    # headers = {"Content-Type": "application/json", "Authorization": "Bearer " + token}
    searchword_api = "http://10.4.32.223:8085/api/v3/reportForm/realtime/category"
    temp_data = dict(data)
    s = requests.Session()
    req = s.post(url=searchword_api, json=temp_data)
    apiresult = json.loads(req.content.decode('utf-8'))
    apiresult_list = apiresult['payload']['modelList']

    apidata = {}
    if len(apiresult_list) > 0:
        # 指标键值对集合

        for ele in apiresult_list:
            key=ele['name']+"_"+ele['path']
            ele.pop('children')
            ele.pop('name')
            ele.pop('path')
            value=util.json_format(ele,'-')
            apidata[key]=value

    return apidata

data={
                                'queryDate': '2021-02-06',
                                'bizType': 5,
                                'source': '1',
                                'platform': '1,2',
                                'fromPlatform': '2,3,7',
                                'mgtType': 0,
                                'categoryPath': '93.16.00.00.00.00',
                                'startTime':'00:00',
                                'endTime':'12:00'

}

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
        # for source in ele['source']:
            source=ele['source']
            if len(source)>1:
                source='2'
            else:
                platform.append((source, ele['platform'], ele['fromPlatform']))
            templist = get_all_platform_list(level + 1, ele['id'])
            platform.extend(templist)

    return platform
# platform_list=get_all_platform_list(1,'')
# print(platform_list)
# sql.report_sql_sdzf_info(data)
apidata=api.report_api_post(data)
UV_sqldata=sql.report_sql_uv(data)
sqldata=sql.report_sql(data)
diff_key_value=util.diff_dict(apidata,sqldata)
print(diff_key_value)
a=1