


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