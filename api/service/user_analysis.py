from resources import map
from utils import util
from utils.date import datechange
from utils.tb_hb import get_tb_hb_key_dict,tb_hb_name_dict
from ._api import request,get_tb_hb_item,item_drillpage,user_drillpage_item
from utils.date import get_enddate_in_w_m_q

#

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


def api_user_analysis_overview_op(url,data,test_indicator_dict=None,is_novalue_key_show=False,defaultvalue='--'):
    '''用户分析优化首页'''
    ''''''
    datacopy=dict(data)
    datacopy['view'] = 'core_index'
    datetype=datacopy['date_type']

    #根据datetype，获取同环比键
    tbhb_keydict=get_tb_hb_key_dict(datetype)

    if datetype not in ['d','h']:
        datacopy['date']=datechange(datacopy['date_type'],datacopy['date'])
    if datetype == 'h':
        url=url.replace('v6','v5')

    # api requests
    overviewinfo = {}
    rawdata = request(url, datacopy)

    try:
        datalist = rawdata['data']['list']
    except Exception as e:
        print(e.__repr__())
        return overviewinfo

    keys = test_indicator_dict.keys()     #测试指标
    if len(datalist) > 0:                 #查询失败的返回{}
        for list_item in datalist:        #取新增和活跃
            for ele in list_item['sub']:

                valuedict = {}

                ename=ele['ename']
                cname = ele['name']
                if test_indicator_dict.__contains__(ename):

                    if ele.__contains__('value_ori') and ele['value_ori'] !='--': #value值缺失，则跳过该指标
                        # _value_ori = ele['value_ori']
                        # valuedict[tb_hb_name_dict['value_ori']] = util.format_precision(_value_ori, selfdefine='--')

                        # 值同环比项
                        valuedict.update(get_tb_hb_item(ele,tbhb_keydict,is_novalue_key_show,defaultvalue))


                    if valuedict=={}:
                        if is_novalue_key_show :
                            overviewinfo[cname] = {}
                    else:
                        overviewinfo[cname]=valuedict


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
            datadict = request(url, datacopy)

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


def api_user_analysis_drill_op(url,data,indicator_name,is_novalue_key_show=False,defaultvalue='--'):
    '''用户分析下钻页'''
    datacopy = dict(data)
    # datacopy['date']=datacopy['date']
    # datacopy.pop('date_str')

    api_drill_data = {}

    field=datacopy['field_str']

    #周的话，替换为周末那一天
    if datacopy['date_type']=='w':
        datacopy['date']=get_enddate_in_w_m_q(datacopy['date'],'w')

    fieldinfo={}
    itemlist=user_drillpage_item(datacopy,field)
    for item in itemlist:
        datacopy['view']=item
        apiinfo={}
        datadict = request(url, datacopy)

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

                    if ele.__contains__('value') and ele['value'] !='--' :
                        # valuedict[tb_hb_name_dict['value_ori']] = util.format_precision(ele['value_ori'],selfdefine='--')
                        #获取同环比项
                        valuedict.update(get_tb_hb_item(ele,tb_hb_name_dict,novaluekeyshow=False,defaultvalue=False))

                    if valuedict == {}:
                        if is_novalue_key_show :
                            apiinfo[name] = {}
                    else:
                        apiinfo[name] = valuedict

            fieldinfo[item]=apiinfo

    api_drill_data[indicator_name] = fieldinfo

    return api_drill_data