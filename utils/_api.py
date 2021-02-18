from . import util
from ._sql import is_platform_show
tb_hb_name_dict = {
    'value_ori': 'value',
    'value':'value',
    'tb_week': '同比上周',
    'tb_year': '同比去年',
    'value_hb': '环比'
}

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