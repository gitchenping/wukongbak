import util


cf=util.readini('./config.ini')
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
