from utils import util
from utils import date
from utils import map
from utils._api import *

#
tb_hb_name_dict = {
    'value_ori': 'value',
    'value':'value',
    'tb_week': '同比上周',
    'tb_year': '同比去年',
    'value_hb': '环比'
}

cf= util.readini('./config.ini')
#经营分析
url_host_jingyingfenxi_realtime=cf.get('api','jingying_realtime_api')
#用户分析
url_host_user_analysis=cf.get('api','user_analysis_api')


def api_user_analysis_overview(data,zhibiao_dict=None):
    '''用户分析首页'''
    datacopy=dict(data)
    # datacopy['date']=datacopy['date_str']
    datacopy.pop('date_str')                #可以免传时间参数

    data['view']='core_index'
    # api requests
    rawdata = util.request(url_host_user_analysis, data)

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

def api_user_analysis_drill(data,zhibiao_dict=None):
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
            datadict = util.request(url_host_user_analysis, datacopy)

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


def api_jingyingfenxi_overview(jingying_analysis_api_path,data):
    '''首页'''
    datacopy = dict(data)
    datacopy['view']='core_index'

    #date_type、date_str键替换
    datetype=datacopy['date_type']
    date=util.datechange(datetype,datacopy['date_str'])

    datacopy['date']=date
    datacopy.pop('date_str')
    datacopy['date_type'] = datetype[0]

    #api requests
    rawdata=util.request(jingying_analysis_api_path,datacopy)

    # 取list
    overviewinfo = {}
    try:
        datalist = rawdata['data']['list']
    except Exception as e:
        print(e)
        return

    #
    tb_hb_name_dict={
        'value_ori':'value',
        'tb_week':'同比上周',
        'tb_year':'同比去年',
        'value_hb':'环比'
    }

    if len(datalist)>0:
        for ele in datalist:
            apiinfo = {}
            # 组装为字典，并返回
            subinfolist = ele['sub']

            for subinfo in subinfolist:
                valuedict= {}
                name = subinfo['name']
                if subinfo.__contains__('value_ori') :
                    if  subinfo['value_ori']!=0:
                        _value_ori = subinfo['value_ori']
                        valuedict[tb_hb_name_dict['value_ori']]=_value_ori
                    else:
                        valuedict[tb_hb_name_dict['value_ori']] = '--'

                if subinfo.__contains__('tb_week') and  isinstance(subinfo['tb_week'] , str):
                    _tb_week = subinfo['tb_week'].strip('%')

                    valuedict[tb_hb_name_dict['tb_week']]=util.format_trans(_tb_week)

                if subinfo.__contains__('tb_year') and  isinstance(subinfo['tb_year'] , str):
                    _tb_year = subinfo['tb_year'].strip('%')
                    valuedict[tb_hb_name_dict['tb_year']]=util.format_trans(_tb_year)
                if subinfo.__contains__('value_hb') and  isinstance(subinfo['value_hb'] , str):
                    _value_hb = subinfo['value_hb'].strip('%')
                    valuedict[tb_hb_name_dict['value_hb']]=util.format_trans(_value_hb)

                overviewinfo[name] = valuedict
        overviewinfo.pop('出库用户转化率')
    else:
        for ele in map.zf_zhibiao:
            overviewinfo[ele]={}
        if datacopy['shop_type']=='2':
            overviewinfo.pop('毛利率')
            overviewinfo.pop('毛利额')

    return overviewinfo

def api_jingyingfenxi_drill(jingying_analysis_api_path,data):
    '''下钻页'''
    datacopy = dict(data)

    # date_type、date_str键替换
    datetype = datacopy['date_type']
    # date = date.datechange(datetype, datacopy['date_str'])

    datacopy['date'] = datacopy['date_str']
    # datacopy['date'] = '2020-07-31'
    datacopy.pop('date_str')
    datacopy['date_type'] = datetype[0]

    sd_zf_ck=datacopy['sale_type']
    if sd_zf_ck=="sd":
        name="收订"
    elif sd_zf_ck=='zf':
        name="支付"
    else:
        name="出库"

    #所有首页指标
    drill_dict={}
    drill_dict.update(map.drill_common_dict)

    if sd_zf_ck!='ck':     #收订和支付有取消率
        drill_dict.update(variable.drill_cancel_dict)
    else:
        drill_dict.update(variable.drill_ck)
    if sd_zf_ck!='sd':
        drill_dict.update(variable.drill_zf_ck_dict)

    api_drill_data={}

    for field in drill_dict.keys():
    # for field in ['cancel_rate']:
        datacopy['field_str']=field
        api_drill_data[name+drill_dict[field]]=jingyingfenxi(jingying_analysis_api_path,datacopy)

    return api_drill_data
    pass


def jingyingfenxi(jingying_analysis_api_path,data):

    datacopy=dict(data)
    # 依次请求趋势、部门分布、平台分布、用户分布、城市排名
    source=datacopy['source']
    parentplatform=datacopy['parent_platform']
    platform=datacopy['platform']
    datetype=datacopy['date_type']

    itemslist = ['trend', 'bd', 'customer']
    if source in ['2','3','4']:    #天猫、抖音、拼多多下钻页没有平台分布
        pass
    elif source=='all':
        itemslist = ['trend', 'bd','platform','customer']
    else:
        if parentplatform in ['1'] and platform=='all': # 轻应用\H5\PC没有平台分布
            itemslist = ['trend', 'bd', 'platform', 'customer']

    drill_data={}
    # itemslist = ['trend']
    #

    for item in itemslist:
        datacopy['view']=item

        apiinfo = {}
        finalurl =jingying_analysis_api_path

        datadict = util.request(finalurl, datacopy)

        if datadict!=-1:

            if item == 'trend':

                # 取value
                try:
                    datalist = datadict['data'][0]['values']
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
                    name=ele['name']

                    if ele.__contains__('value'):
                        valuedict[tb_hb_name_dict['value']] = util.tb_hb_format(ele['value'])

                    if ele.__contains__('tb_week'):
                        valuedict[tb_hb_name_dict['tb_week']] = util.tb_hb_format(ele['tb_week'])

                    if ele.__contains__('tb_year'):
                        valuedict[tb_hb_name_dict['tb_year']] = util.tb_hb_format(ele['tb_year'])

                    if ele.__contains__('value_hb'):
                        valuedict[tb_hb_name_dict['value_hb']] = util.tb_hb_format(ele['value_hb'])

                    apiinfo[name]=valuedict

            drill_data[item] = apiinfo

    return drill_data

#异常数据提取
def jingying_analysis(datetype, date, logger):
    '''经营分析'''
    for source in ['all','1','2','3','4']:   #平台来源      all-all 1主站 2天猫 3抖音 4拼多多
        if source !='1':
            parent_platformlist = ['all']
        else:  # 点击主站可以下钻APP、轻应用、H5、PC
            parent_platformlist = ['1', '2', '3', '4']

        for parent_platform in parent_platformlist:

            if parent_platform == '1':
                platformlist = ['1', '2']  # android 、 ios

            elif parent_platform == '2':
                platformlist = ['all', '3', '4', '5', '6', '7', '8', '9']  # all、快应用、微信小程序、百度小程序、头条、qq、360
            else:
                platformlist = ['all']

            for platform in platformlist:

                for bd_id in ['all', '1', '2']:  # 事业部id：all-all 1-出版物事业部 2-日百服 3-数字业务事业部 4-文创

                    for shop_type in ['all', '1', '2']:  # 经营方式 all-ALL 1-自营 2-招商

                        for eliminate_type in ['all', '1']:  # 剔除选项 all-all 1-剔除建工

                            for sale_type in ['sd', 'zf', 'ck']:  # 收订sd、支付zf、出库ck

                                data = {'source': source, 'parent_platform': parent_platform, 'platform': platform,
                                        'bd_id': bd_id, 'shop_type': shop_type, 'eliminate_type': eliminate_type,
                                        'sale_type': sale_type, \
                                        'date_type': datetype, 'date_str': date}
                                # data={'source': '1', 'parent_platform': '5', 'platform': 'all',
                                #     'bd_id': '4', 'shop_type': '2', 'eliminate_type': '1','sale_type':'zf' ,\
                                #     'date_type':'d','date_str': '2020-11-03'}
                                if data['source'] != '1' and data['shop_type'] == '2':
                                    continue

                                # 首页
                                util.jingying_request(jingying_analysis_overview_api_url, data, logger)
