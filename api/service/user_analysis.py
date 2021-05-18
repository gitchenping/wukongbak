from resources import map
from utils import util
from utils.date import datechange
from ._api import request,get_tb_hb_item,item_drillpage

#
tb_hb_name_dict = {
    'value_ori': 'value',
    'value':'value',
    'tb_week': '同比上周',
    'tb_year': '同比去年',
    'value_hb': '环比'
}



def api_user_analysis_overview(url,data,zhibiao_dict=None):
    '''用户分析首页'''
    datacopy=dict(data)
    # datacopy['date']=datacopy['date_str']
    datacopy.pop('date_str')                #实时分析可以免传时间参数

    data['view']='core_index'
    # api requests
    rawdata = request(url, data)

    # 取list
    overviewinfo = {}
    try:
        datalist = rawdata['data']['list']
    except Exception as e:
        print(e)
        return

    # 取活跃用户中的收订、支付和出库
    keys=zhibiao_dict.keys()
    if len(datalist)>0:
        for ele in datalist[1]['sub']:

            valuedict={}
            if zhibiao_dict is not None:
                if ele.__contains__('ename') and ele['ename'] in keys:
                    name = ele['name']
                    if ele.__contains__('value_ori') :
                        if  ele['value_ori']!=0:
                            _value_ori = ele['value_ori']
                            valuedict[tb_hb_name_dict['value_ori']]=util.format_precision(_value_ori,selfdefine='--')
                        else:
                            valuedict[tb_hb_name_dict['value_ori']] = '--'
                    #同环比项
                    valuedict.update(get_tb_hb_item(ele))
                    overviewinfo[name] = valuedict
    else:
        for ele in zhibiao_dict.keys():
            overviewinfo[ele]={}
    return overviewinfo


def api_user_analysis_overview_op(url,data,zhibiao_dict=None):
    '''用户分析优化首页'''
    datacopy=dict(data)

    datacopy['date']=datechange(datacopy['date_type'],datacopy['date'])


    datacopy['view'] = 'core_index'
    # api requests
    rawdata = request(url, datacopy)

    # 取list
    overviewinfo = {}
    try:
        datalist = rawdata['data']['list']
    except Exception as e:
        print(e)
        return

    # 取新增和活跃
    keys = zhibiao_dict.keys()
    if len(datalist) > 0:
        for list_item in datalist:
            for ele in list_item['sub']:

                valuedict = {}
                if zhibiao_dict is not None:
                    if ele.__contains__('ename') and ele['ename'] in keys:
                        name = ele['name']
                        if ele.__contains__('value_ori'):
                            if ele['value_ori'] != 0:
                                _value_ori = ele['value_ori']
                                valuedict[tb_hb_name_dict['value_ori']] = util.format_precision(_value_ori, selfdefine='--')
                            else:
                                valuedict[tb_hb_name_dict['value_ori']] = '--'
                        # 同环比项
                        valuedict.update(get_tb_hb_item(ele))
                        overviewinfo[name] = valuedict
    else:
        for ele in zhibiao_dict.keys():
            overviewinfo[ele] = {}

    if datacopy['shop_type'] !='all' or datacopy['bd_id'] !='all':
        if overviewinfo.__contains__('新增注册用户'):
            overviewinfo.pop('新增注册用户')

    return overviewinfo




def api_user_analysis_drill(url,data,zhibiao_dict=None):
    '''用户分析下钻页'''
    datacopy = dict(data)
    datacopy['date']=datacopy['date_str']
    datacopy.pop('date_str')

    api_drill_data = {}

    for field in zhibiao_dict.keys():

        datacopy['field_str'] = field
        fieldinfo={}
        for item in item_drillpage(datacopy):
            datacopy['view']=item
            apiinfo={}
            datadict = util.request(url, datacopy)

            if datadict!=-1:
                if item == 'trend':
                    # 取value
                    try:
                        datalist = datadict['data'][0]['values']           #取当日数据
                        for ele in datalist:
                            apiinfo[ele[0]] = round(float(ele[1]), 2)
                    except Exception as e:
                        apiinfo = {}
                else:

                    # 取data
                    if item == 'app':
                        datalist = datadict['data'][0:10]
                    else:
                        datalist = datadict['data']

                    for ele in datalist:
                        valuedict = {}
                        name = ele['name']

                        if ele.__contains__('value'):
                            valuedict[tb_hb_name_dict['value']] = util.format_precision(ele['value'],selfdefine='--')
                        #获取同环比项
                        valuedict.update(get_tb_hb_item(ele))
                        apiinfo[name] = valuedict
                fieldinfo[item]=apiinfo

        api_drill_data[zhibiao_dict[field]] = fieldinfo

    return api_drill_data


def api_user_analysis_drill_op(url,data,zhibiao_dict=None):
    '''用户分析下钻页'''
    datacopy = dict(data)
    datacopy['date']=datacopy['date_str']
    datacopy.pop('date_str')

    api_drill_data = {}

    for field in zhibiao_dict.keys():

        datacopy['field_str'] = field
        fieldinfo={}
        for item in item_drillpage(datacopy):
            datacopy['view']=item
            apiinfo={}
            datadict = util.request(url, datacopy)

            if datadict!=-1:
                if item == 'trend':
                    # 取value
                    try:
                        datalist = datadict['data'][0]['values']           #取当日数据
                        for ele in datalist:
                            apiinfo[ele[0]] = round(float(ele[1]), 2)
                    except Exception as e:
                        apiinfo = {}
                else:

                    # 取data
                    if item == 'app':
                        datalist = datadict['data'][0:10]
                    else:
                        datalist = datadict['data']

                    for ele in datalist:
                        valuedict = {}
                        name = ele['name']

                        if ele.__contains__('value'):
                            valuedict[tb_hb_name_dict['value']] = util.format_precision(ele['value'],selfdefine='--')
                        #获取同环比项
                        valuedict.update(get_tb_hb_item(ele))
                        apiinfo[name] = valuedict
                fieldinfo[item]=apiinfo

        api_drill_data[zhibiao_dict[field]] = fieldinfo

    return api_drill_data