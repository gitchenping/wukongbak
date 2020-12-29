from util import util

cf= util.readini('./config.ini')
url_host_jingyingfenxi_realtime=cf.get('api','jingying_realtime_api')

def jingyingfenxi(data):

    datacopy=dict(data)
    # 依次请求趋势、部门分布、平台分布、用户分布、城市排名
    source=datacopy['source']
    parentplatform=datacopy['parent_platform']
    platform=datacopy['platform']

    itemslist = ['trend', 'bd', 'customer']
    if source in ['2','3','4']:    #天猫、抖音、拼多多下钻页没有平台分布
        pass
    elif source=='all':
        itemslist = ['trend', 'bd','platform','customer']
    else:
        if parentplatform in ['1'] and platform=='all': # 轻应用\H5\PC没有平台分布
            itemslist = ['trend', 'bd', 'platform', 'customer']

    drill_data={}

    trend_key=datacopy['date'].split(' ')[-1]+"点"
    for item in itemslist:
        datacopy['view']=item

        apiinfo = {}
        finalurl =url_host_jingyingfenxi_realtime

        datadict = util.request(finalurl, datacopy)

        if datadict and datadict['code'] == 200:

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
                    key = ele['name']
                    if ele['value'] is not None and ele['value'] != '--':  # 如果值为空，跳过该key
                        apiinfo[key] = round(float(ele['value']), 2)

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
