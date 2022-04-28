import requests
from utils import util
import json
from utils.decorate import retry



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

def user_drillpage_item(data,indicator=None):
    '''用户分析下钻页item'''
    itemslist = ['trend','platform']

    if not is_platform_show(data):
        itemslist.pop(itemslist.index('platform'))

    #['uv','new_create_parent_uv_sd','new_create_parent_uv_zf','new_create_parent_uv_ck','create_parent_uv_sd',
                    # 'create_parent_uv_zf','create_parent_uv_ck','daycount_ratio_sd','daycount_ratio_zf']
    if indicator not in ['uv','new_uv','new_uv_ratio','register_number']:
        itemslist.append('bd')

    if indicator in ['uv','create_parent_uv_sd','create_parent_uv_zf','create_parent_uv_ck','daycount_ratio_sd','daycount_ratio_zf']:
        if indicator =='uv':
            itemslist.append('uv')
        else:
            itemslist.append('customer')

    if indicator in ['daycount_ratio_sd','daycount_ratio_zf']:
        itemslist.append('quantile')
    return itemslist


def get_tb_hb_item(data,tb_hb_name_dict,novaluekeyshow,defaultvalue):
    '''返回api请求结果中的同环比键值对'''
    ele=dict(data)

    valuedict={}

    for key,value in tb_hb_name_dict.items():
        if ele.__contains__(key):
            valuedict[value] =ele[key]
        else:
            if novaluekeyshow:     #显示缺失值
                valuedict[value]=defaultvalue

    valuedict=util.json_format(valuedict,defaultvalue)

    return valuedict