

parentchannel={
    '1':'应用市场','2':'快应用渠道','3':'精准投放','4':'联盟','5':'分享活动','6':'搜索','7':'品专','8':'广告','all':'全部'
}
parentsecondchannel={
    '1':{'1':'华为智汇云','2':'小米','3':'OPPO市场','4':'VIVO商店','5':'魅族','6':'百度','7':'360助手',
                               '8':'应用宝（自然）','9':'应用宝（付费）','10':'UC浏览器（付费）','11':'其他','12':'App Store'
         },
    '2':{'1':'华为快应用','2':'OPPO快应用','3':'VIVO快应用','4':'小米快应用','5':'魅族快应用',
                                        '6':'努比亚快应用','7':'其他快应用'
         },
    '3': {'all':'全部'},
    '4': {'1':'手机联盟', '2':'评论联盟', '3':'微信联盟'
          },
    '5': {'1':'打卡', '2':'0元领', '3':'一分抽奖', '4':'步数赚钱', '5':'天天领现金', '6':'1元砍价', '7':'读书计划',
            '8':'抓娃娃', '9':'答题领红包', '10':'当当抽大奖', '11':'助力免单'},
    '6': {'1':'SEM', '2':'免费搜索'},
    '7': {'1':'百度品专', '2':'搜狗品专', '3':'神马品专', '4':'360品专'},
    '8': {'1':'app广告', '2':'微信小程序广告'},
    'all':{'all':'全部'}
}
channel_default={'all':'全部'}
channel_search={'1':{'1':'百度SEM','2':'神马SEM','3':'360SEM','4':'搜狗SEM'},
                    '2':{'1':'搜狗免费搜索','2':'神马免费搜索','3':'360免费搜索','4':'百度免费搜索'}}


'''
for parentchannel in channeldict.keys():
    a=parentchannel
    aa=dict(list(channeldict[a].values())[0])
    for parentsecondchannel in aa.keys():
        b = parentsecondchannel
        bb=dict(list(list(aa.values())[0].values())[0])
        for channel in bb.keys():
            c=channel
            # cc=list(c.keys())[0]
        print(a,b,c)
'''


#渠道拆解
def channel_site(datetype, date):
    '''渠道拆解'''
    data={}
    for _parentchannel in parentchannel.keys():
        for _parentsecondchannel in parentsecondchannel[_parentchannel].keys():
            if _parentchannel == '6':
                channeldict = channel_search[_parentsecondchannel]
            else:
                channeldict = channel_default
            for _channel in channeldict.keys():
                channel = _channel
                # print(_parentchannel, _parentsecondchannel, _channel)
                for user_type in ['all', '1', '2']:  # 1-新房客，2-老访客
                    data = {'source': 'all', 'parent_platform': 'all', 'platform': 'all',
                            'user_type': user_type, 'date_type': datetype, 'date': date, 'parent_channel': _parentchannel,
                            'parent_second_channel': _parentsecondchannel, 'channel': channel}
                    print(data)
                    # data = {'source': 'all', 'parent_platform': 'all', 'platform': 'all',
                    #         'user_type': user_type, 'date_type': datetype, 'date': date, 'parent_channel': '8',
                    #         'parent_second_channel': 'all', 'channel': 'all'}

                    apidata = api.client_analysis_api(data, dwmkey,'site')
                    sqldata = sql.channel_site_sql(data, dwmkey)