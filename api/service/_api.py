import requests
from utils import util
import json

'''请求重试'''
def retry(maxretry=3):
    def decorator(func):
        def wrapper(*args, **kw):
            att = 0
            while att < maxretry:
                try:
                    return func(*args, **kw)
                except Exception as e:
                    att += 1
        return wrapper
    return decorator

@retry(2)
def request(url,data=None,token=None):
    headers=None
    if token is not None:
        headers={'Authorization':'Bearer MDAwMDAwMDAwMLGenKE'}
    textdata=requests.get(url,params=data,headers=headers)

    if textdata is not None and textdata.status_code==200:
        return json.loads(textdata.text)

    #请求失败
    return -1


@retry(2)
def post(url,data=None,headers=None,token=None):
    # headers = {"Content-Type": "application/json", "Authorization": "Bearer " + token}
    header={}
    if headers is None:    #默认
        header["Content-Type"]="application/json;charset=UTF-8"

    if token is not None:
        header['Authorization']="Bearer "+token

    s = requests.Session()
    #实参data 为dict时，若header为application/json，使用json参数；若为application/x-www-form，使用data参数
    if header["Content-Type"]=="application/json;charset=UTF-8":
        req = s.post(url=url, json=data, headers=header)
    else:
        req = s.post(url=url, data=data, headers=header)

    if req.status_code == 200:
        res_data=req.content
        if isinstance(res_data,bytes):
            res_data =res_data.decode('utf-8')
        return res_data
    return -1



tb_hb_name_dict = {
    'value_ori': 'value',
    'value':'value',
    'tb_week': '同比上周',
    'tb_year': '同比去年',
    'value_hb': '环比'
}

def is_platform_show(data):
    '''下钻页平台是否显示'''
    datacopy=dict(data)

    source = datacopy['source']
    parentplatform = datacopy['parent_platform']
    platform = datacopy['platform']

    show=True                                               #默认显示
    if source not in ['2', '3', '4']:                       # 天猫、抖音、拼多多下钻页没有平台分布
        if parentplatform not in ['2', '3', '4']:           # 轻应用\H5\PC没有平台分布

            if parentplatform != 'all':
                if platform =='all':                    # 安卓、IOS没有平台分布
                    show=True
                else:
                    show=False
            else:
                show=True
        else:
            show = False
    else:
        show= False
    return show

def item_drillpage(data=None):
    '''返回下钻页可以展示的条目名称列表'''
    itemslist = ['trend', 'bd', 'customer']
    if is_platform_show(data):
        itemslist=['trend', 'bd','platform', 'customer']

    return itemslist

def get_tb_hb_item(data):
    '''返回api请求结果中的同环比键值对'''
    ele=dict(data)
    valuedict={}
    if ele.__contains__('tb_week'):
        _tb_week = ele['tb_week']
        valuedict[tb_hb_name_dict['tb_week']] = util.format_precision(_tb_week, selfdefine='--')

    if ele.__contains__('tb_year'):
        _tb_year = ele['tb_year']
        valuedict[tb_hb_name_dict['tb_year']] = util.format_precision(_tb_year, selfdefine='--')

    if ele.__contains__('value_hb'):
        _value_hb = ele['value_hb']
        valuedict[tb_hb_name_dict['value_hb']] = util.format_precision(_value_hb, selfdefine='--')

    return valuedict