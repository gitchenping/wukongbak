'''
实时搜索2期 map
'''
import operator

main_table ={
    'clickPv': '点击PV',
    'clickPvLinkRelative': '点击PV环比',
    'clickPvWoW': '点击PV同比上周',
    'clickPvYoY': '点击PV同比去年',
    'clickUv': '点击UV',
    'clickUvLinkRelative': '点击UV环比',
    'clickUvWoW': '点击UV同比上周',
    'clickUvYoY': '点击UV同比去年',
    'createCustNum': '收订用户数',
    'createCustNumLinkRelative': '收订用户数环比',
    'createCustNumWoW': '收订用户数同比上周',
    'createCustNumYoY': '收订用户数同比去年',
    'createParentNum': '收订订单数',
    'createParentNumLinkRelative': '收订订单数环比',
    'createParentNumWoW': '收订订单数同比上周',
    'createParentNumYoY': '收订订单数同比去年',
    'createSaleAmt': '收订金额',
    'createSaleAmtLinkRelative': '收订金额环比',
    'createSaleAmtWoW': '收订金额同比上周',
    'createSaleAmtYoY': '收订金额同比去年',
    'mainProductId': '主品ID',

    'path2Name': '二级类',
    'rpm': 'RPM',
    'rpmLinkRelative': 'RPM环比',
    'rpmWoW': 'RPM同比上周',
    'rpmYoY': 'RPM同比去年',
    'searchAmtRate': '搜索UV价值',
    'searchAmtRateLinkRelative':'搜索UV价值环比',
    'searchAmtRateWoW':'搜索UV价值同比上周',
    'searchAmtRateYoY':'搜索UV价值同比去年',
    'searchClickRate': '搜索UV点击率',
    'searchCustRate': '收订用户转化率',
    'searchPv': '搜索PV',
    'searchPvLinkRelative': '搜索PV环比',
    'searchPvWoW': '搜索PV同比上周',
    'searchPvYoY': '搜索PV同比去年',
    'searchUv': '搜索UV',
    'searchUvLinkRelative': '搜索UV环比',
    'searchUvWoW': '搜索UV同比上周',
    'searchUvYoY': '搜索UV同比去年',
    'searchWord': '搜索词',

    }

sql_table = {
     'search_word':'搜索词',
     'path2_name':'二级分类名称',
     'main_product_id' :'主品ID',
     'search_uv': '搜索uv',
     'search_pv':'发起搜索请求的次数',
     'click_uv':'点击uv',
     'click_pv' :'点击次数',
     'create_cust_num' :'收订用户数',
     'create_parent_num':'收订订单数',
     'create_sale_amt' :'收订金额'
}

anomaly = {
    # '':'全部',
     "1":'仅异动'
}

comparetype ={
    1:'YoY',
    2:'WoW',
    3:'LinkRelative'
}

bd_id2name ={
    '0':'全部',
    '1':'出版物',
    '2':'百货',
    '6':'服装',
    '4':'文创',
    '3':'数字'
}

word_select_time ={
    '1':">=1",
    '2':">=2",
    '3':">=3",
    '4':">=4",
    '5':">=5",
    '6':">=6",
    '7':">=7",
    '8':">=8",
    '9':">=9",
    '10':">=10",
}

tbhb2name = {
    'number':'value',
    'hb':'LinkRelative',
    'hb_lastweek':'WoW',
    'hb_lastyear':'YoY'
}

abnomaly_field ={
    'spv':'searchPv',
    'suv':'searchUv',
    'cpv':'clickPv',
    'cuv':'clickUv',
    'csa':'createSaleAmt',
    'cpn':'createParentNum',
    'ccn':'createCustNum',
    'scust_rate':'searchCustRate',
    'sclick_rate':'searchClickRate',
    'samt_rate':'searchAmtRate',
     'rpm_rate':"rpm"
}

abnomaly_field_cal_map ={
    'searchPv':'search_pv as searchPv',
    'searchUv':'search_uv as searchUv',
    'clickPv':'click_pv as clickPv',
    'clickUv':'click_uv as clickUv',
    'createSaleAmt':'create_sale_amt as createSaleAmt',
    'createParentNum':'create_parent_num as createParentNum',
    'createCustNum':'create_cust_num as createCustNum',
    'searchCustRate':" case when search_uv <>0 then round(create_cust_num/ search_uv ,2) else null end as searchCustRate",
    'searchClickRate':" case when search_uv <>0 then round(click_uv / search_uv , 2) else null end as searchClickRate",
    'searchAmtRate':" case when search_uv <>0 then round(create_sale_amt / search_uv , 2) else null end as searchAmtRate",
    'rpm':" case when search_pv <>0 then round(create_sale_amt / search_pv * 1000, 2) else null end as rpm"
}

ops={
    # '=':operator.eq,
    # '>':operator.gt,
    # '<':operator.lt,
    '>=':operator.ge,
    # '<=':operator.le,
    # '<>':operator.ne
}
