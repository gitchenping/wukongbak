from ._api import post
import json
def reco_api_data(token,requestdata):
    url = "http://newwk.dangdang.com/api/v3/views/72/getdata"
    apidata=post(url,requestdata,token=token)

    rawdata={}
    if apidata != -1:
        jsondata = json.loads(apidata)
        rawdata = jsondata['payload']['resultList']

    return rawdata

    pass