import requests,json
from utils import util,log

@util.retry(2)
def report_api_post(data):
    # headers = {"Content-Type": "application/json", "Authorization": "Bearer " + token}
    searchword_api = "http://10.4.32.223:8085/api/v3/reportForm/realtime/category"
    temp_data = dict(data)
    s = requests.Session()
    req = s.post(url=searchword_api, json=temp_data)
    apiresult = json.loads(req.content.decode('utf-8'))
    apiresult_list = apiresult['payload']['modelList']

    apiresult_list_sort=sorted(apiresult_list,key=lambda x:x['path'])

    #post处理
    apidata={}
    rtnapidata={}
    count=0
    if len(apiresult_list_sort)>0:
        #指标键值对集合
        for ele in apiresult_list_sort:         #取前两个key-value对
            # key=ele['name']+"_"+ele['path']
            if ele['subsAmount'] !='0.00' :
                key=ele['path']
                ele.pop('children')
                ele.pop('name')
                ele.pop('path')
                value=util.json_format(ele,'-')
                rtnapidata[key]=value
                count+=1
            if count==2:
                break

    return rtnapidata