from resources import map


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
        drill_dict.update(map.drill_cancel_dict)
    else:
        drill_dict.update(map.drill_ck)
    if sd_zf_ck!='sd':
        drill_dict.update(map.drill_zf_ck_dict)

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
