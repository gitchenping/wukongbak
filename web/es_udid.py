import requests
import json









if __name__ == '__main__':

    udid_list = []
    url_risk = "http://10.5.38.65:9200/risk_ddudid_device/_search?size=300&q=*:*"
    url_udid = "http://10.5.38.65:9200/device_ddudid_custid/_search"

    params_data ={
        "query":{
            "match_all":{}
        }
    }

    r = requests.get(url_risk)

    if r.status_code == 200 :
        dict_data = json.loads(r.text)
        sources = dict_data['hits']['hits']
        if len(sources) >0:

            udid_list = [ele['_source']['udid'] for ele in sources]

            for udid in udid_list:
                url_udid_search = url_udid+"?q=udid:"+udid
                r = requests.get(url_udid_search)
                if r.status_code == 200:
                    dict_data = json.loads(r.text)
                    sources = dict_data['hits']['hits']
                    if len(sources) > 0:
                        print(sources)
                        cust_id = [ele['_source']['cust_id'] for ele in sources]
                        print(cust_id)
    pass