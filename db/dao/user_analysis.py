from utils import util
from ._sql import is_platform_show,get_bd_where,get_platform_where,get_time_where,get_shoptype_where,get_eliminate_where,get_drill_tb_hb
from ._sql import get_sql_for_user_analysis_overview_op,get_overview_tb_hb
from ._sql import get_where_for_analysis_overview_op,is_show_for_user_drill,get_plat_column,get_bd_column
from utils import tb_hb
from utils.date import get_trend_where_date,get_trend_data
import numpy as np
import requests
import re
from resources.map import customer_dict,new_old_customer_dict,bd_id_dict,app_dict,source_dict,parent_platform_dict,cat_name_dict


#用户分支优化各指标计算逻辑
user_indicator_op_cal_dict={
    "new_uv":['uniqExactMerge(device_id_state) as new_uv','mdata_flows_user_realtime_day_all'],  #"新访UV"
    "new_uv_ratio":'new_uv/uv as new_uv_ratio',  #"新访uv占比"
    "register_number":['count(distinct cust_id) as register_number','mdata_customer_new_all'] ,    #"新增注册用户"
    "new_create_parent_uv_sd":['groupBitmapMerge(cust_id_state) as new_create_parent_uv_sd','dm_order_create_day'],  #"新增收订用户"
    "new_create_parent_uv_zf":['groupBitmapMerge(cust_id_state) as new_create_parent_uv_zf','dm_order_pay_day'],             #"新增支付用户"
    "new_create_parent_uv_ck": ['groupBitmapMerge(cust_id_state) as new_create_parent_uv_ck','dm_order_send_day'],             #"新增出库用户"
    "uv":['uniqExactMerge(device_id_state) as uv','mdata_flows_user_realtime_day_all'] ,        #"活跃UV"
    "create_parent_uv_sd":['groupBitmapMerge(cust_id_state) as create_parent_uv_sd','dm_order_create_day'],#"收订用户"
    "create_parent_uv_zf":['groupBitmapMerge(cust_id_state) as create_parent_uv_zf','dm_order_pay_day'],#'"支付用户"
    "create_parent_uv_ck":['groupBitmapMerge(cust_id_state) as create_parent_uv_ck','dm_order_send_day'],#"出库用户",
    "daycount_ratio_sd":['groupBitmapMerge(parent_id_state)/groupBitmapMerge(cust_id_state) as daycount_ratio_sd','dm_order_create_day'],#"收订下单频次",
    "daycount_ratio_zf":['groupBitmapMerge(parent_id_state)/groupBitmapMerge(cust_id_state) as daycount_ratio_zf','dm_order_pay_day'] #"支付下单频次"
}


def sql_user_analysis_overview(data,ck_tables,data_type_dict,conn=None):

    date=data['date_str'].split(' ')[0]
    datetype=data['date_type']

    where=''
    if datetype in ['h','hour']:
        hour_str=data['date_str'].split(' ')[-1]
        where=" data_type in ('1','2','3') and hour_str<= "+"'"+hour_str+"' and "

    where+=get_platform_where(data, yinhao=True)
    where+=get_time_where(data)

    groupby=" group by date_str,data_type"
    orderby=" order by data_type,date_str desc"
    sql=" select data_type,count(distinct cust_id) AS create_parent_uv,date_str from "\
        +ck_tables+" where "+where+groupby+orderby

    conn.execute(sql)
    rawdata=conn.fetchall()

    sqldata={}
    if len(rawdata) > 0:
        sqldata = get_drill_tb_hb(rawdata, data_type_dict, date, datetype)

    return sqldata


def sql_user_analysis_overview_op(data, test_indicator_dict, ck_db=None):

    date_hour = data['date'].split(' ')       #对于时的情况，传值形式为2021-05-21 13

    date=date_hour[0]
    datetype = data['date_type']

    sqldata={}

    #当选择事业部或者经营方式时，新增注册用户数 隐藏不显示
    indicator_dict=dict(test_indicator_dict)
    if data['bd_id'] !='all' or data['shop_type'] !='all':
        indicator_dict.pop('register_number')

    sqldict=get_sql_for_user_analysis_overview_op(data,indicator_dict,user_indicator_op_cal_dict)
    # sql='select 2+2'
    # conn.execute(sql)
    # rawdata=conn.fetchall()

    #依次处理每个指标
    for ename,sql in sqldict.items():

        r=requests.get(headers=ck_db['headers'],url=ck_db['host'],params={'query':sql})

        '''debug
             rawdata=[[360,338,1.14,1.14,'2021-05-12',],
                    [None,273,1.09,1.05,'2021-05-11'],
                    [198,184,1.1,1.04,'2021-05-05'],
                    [216,196,1.06,1.06,'2020-05-12']]

            '''
        if r.status_code == 200 and len(r.text) > 0:
            rtext=r.text.strip('\n')
            rawdata = [ele.split('\t') for ele in rtext.split('\n')]

            each_indicator_list=[[float(ele[0]), ele[1]] for ele in rawdata if
                                       ele[0] is not None and ele[0] != '0']

            each_item_sqldata = get_overview_tb_hb(each_indicator_list, test_indicator_dict[ename], date,
                                                   datetype,misskeyshow=False)
            if each_item_sqldata[test_indicator_dict[ename]]['value']!='--':
                sqldata.update(each_item_sqldata)
        # else:
        #     sqldata[ename]={}

    #新访uv占比
    # if indicator_dict.__contains__('new_uv_ratio') :
    #     new_uv_value=sqldata['新访UV']['value']
    #     uv_value=sqldata['活跃UV']['value']
    #     if uv_value is not None or uv_value !=0 or uv_value!='--':
    #         sqldata['新访UV占比']=round( new_uv_value / uv_value *100 ,2)


    return sqldata

    '''
    
    # 将rawdata转换为narray类型
            raw_data_array = np.array(rawdata)

            i = 0
            for indicator_name in indicator_dict.keys():  # 循环处理每个指标

                each_indicator = raw_data_array[:, [i, -1]]
                each_indicator_list = each_indicator.tolist()
                each_indicator_list = [[float(ele[0]), ele[1]] for ele in each_indicator_list if
                                       ele[0] is not None and ele[0] != '0']

                each_item_sqldata = get_overview_tb_hb(each_indicator_list, test_indicator_dict[indicator_name], date,
                                                       datetype)

                sqldata.update(each_item_sqldata)

                i += 1
                pass


    return sqldata
    '''



def sql_user_analysis_drill(datacopy, ck_tables, test_indicator_dict, data_type_dict,conn=None):
    '''
        用户分析下钻页
    :param datacopy:
    :param ck_tables:
    :param test_indicator_dict:测试指标字典，参考resources/map.py
    :param data_type_dict:
    :param conn:
    :return:
    '''

    sql_data = {}
    date = datacopy['date_str'].split(' ')[0]
    datetype = datacopy['date_type']

    source=datacopy['source']
    parentplatform=datacopy['parent_platform']
    platform=datacopy['platform']

    where = ''
    if datetype in ['h', 'hour']:
        hour_str = date.split(' ')[-1]
        where = " data_type in ('1','2','3') and hour_str<= " + "'" + hour_str + "' and "

        # where = "hour_str<= " + "'" + hour_str + "' and "


    where += get_platform_where(datacopy, yinhao=True)
    where+=get_time_where(datacopy)


    for zhibiao in test_indicator_dict.keys():

        drill_data={}
        if zhibiao=='create_parent_uv_sd':
            datatype='1'
        elif zhibiao=='create_parent_uv_zf':
            datatype = '2'
        else:
            datatype = '3'

        newwhere=where+" and  data_type in ('"+datatype+"')"

        # trend
        column = 'count(distinct cust_id) as create_parent_uv,date_str'
        trend = {}  # 存放trend 结果
        trenwhere=newwhere+" and date_str='"+date+"'"
        trendsql = " select  hour_str," + column + " from " + ck_tables+\
               " where " + trenwhere + " group by hour_str,date_str "

        conn.execute(trendsql)
        ck_data=conn.fetchall()

        if len(ck_data) > 0 and ck_data[0][0] is not None:  # key值换算
            for ele in ck_data:
                key = ele[0] + "点"
                value = ele[1]
                trend[key] = round(value, 2)
        drill_data['trend'] = trend
        #
        # 事业部分布
        bd = {}
        bdnamedict = map.bd_id_dict
        column_bd = "(case  when info_bd_id in('5','12') then '1' " \
                    "when info_bd_id in('1','3','4','9','15','16') then '2' " \
                    "when info_bd_id IN ('3') THEN '6' " \
                    "when info_bd_id in('20','21','23') then '4' " \
                    "else '5' end ) as _bd_id,"

        group_by = " group by _bd_id,date_str"
        order_by = " order by _bd_id ,date_str desc"
        bdsql = " select  " + column_bd + column + " from " + ck_tables+ \
                " where " + newwhere + group_by+ order_by

        conn.execute(bdsql)
        ck_data = conn.fetchall()
        if len(ck_data) > 0:  # key值换算
            bd=get_drill_tb_hb(ck_data, bdnamedict,date,datetype)
        drill_data['bd'] = bd
        #
        #平台分布
        group_by = " group by date_str,"
        plat= {}

        if source == '1' and parentplatform == 'all':
            p_newwhere =" where "+ newwhere.replace("source in ('1') and", '')

            #下钻app的时候子类分别去重再加和
            '''
            column = 'sum(create_parent_uv) as _create_parent_uv,date_str'
            column_plat_inner_sql = "select " + "  source,platform,count(distinct cust_id) AS create_parent_uv,date_str " + \
                                    " from " + ck_tables + p_newwhere + " group by source,platform,date_str"
            ck_tables = "(" + column_plat_inner_sql + ")"
            p_newwhere = ''
            '''

        else:
            p_newwhere = " where "+newwhere

        if is_platform_show(datacopy):
            if source == 'all':
                column_plat = 'source,'
                platdict = map.source_dict

                group_by = group_by + "source "
                order_by = 'order by source'
            elif source=='1' and parentplatform=='all':     #1-all-all
                column_plat ="(case when platform in ('0') then '4' " \
                             "when source='1' and platform in ('12','20') then '3' " \
                             "when source='1' and platform in ('1','2') then '1' else '2' end) as _platform,"

                platdict = map.parent_platform_dict

                group_by = group_by + "_platform "
                order_by = ' order by _platform'

            else:                                     #1-1-all
                column_plat = 'platform,'
                platdict = map.app_dict

                group_by = group_by + "platform "
                order_by = ' order by platform'

            order_by+=',date_str desc'


            platsql = " select  " + column_plat + column + " from " + ck_tables + \
                         p_newwhere + group_by + order_by

            conn.execute(platsql)
            ck_data = conn.fetchall()
            if len(ck_data) > 0:  # key值换算
                plat = get_drill_tb_hb(ck_data, platdict, date, datetype)

            drill_data['platform'] = plat

        #
        #新老客
        customer = {}
        customerdict= map.customer_dict
        groupby_new_flag="new_id"
        order_by = " order by " + groupby_new_flag + "," + "date_str desc"
        group_by = " group by date_str"+ "," + groupby_new_flag

        customersql = "select " + groupby_new_flag + "," + column + " from " + ck_tables\
                      + " where " + newwhere + group_by + order_by

        conn.execute(customersql)
        ck_data = conn.fetchall()
        if len(ck_data) > 0:  # key值换算
            customer = get_drill_tb_hb(ck_data, customerdict, date, datetype)
        drill_data['customer'] = customer

        #
        sql_data[test_indicator_dict[zhibiao]]=drill_data

    return sql_data


def sql_user_analysis_drill_op(datacopy,ck_db,ename='',cname=''):
    '''
        用户分析下钻页
    :param datacopy:
    :param ck_tables:
    :param test_indicator_dict:测试指标字典，参考resources/map.py
    :param data_type_dict:
    :param conn:
    :return:
    '''

    sql_drill_data = {}
    itemdict={}
    date_type=datacopy['date_type']
    if date_type == 'd':
        column_date = 'date_str'
        new_flag="day"
    elif date_type == 'w':
        column_date = 'toStartOfWeek(toDate(date_str), 1) as date_str'
        new_flag = "week"
    elif date_type == 'm':
        column_date = 'toStartOfMonth(toDate(date_str)) as date_str'
        new_flag = "month"
    else:
        column_date = 'toStartOfQuarter(toDate(date_str)) as date_str'
        new_flag = "quarter"


    source = datacopy['source']
    parentplatform = datacopy['parent_platform']

    date = datacopy['date'].split(' ')[0]
    datetype=datacopy['date_type']

    where=get_where_for_analysis_overview_op(datacopy,ename)

    column = user_indicator_op_cal_dict[ename][0] + "," + column_date

    table_alias=''
    if datetype!='d':
        table_alias=' t'
    table="bi_mdata."+user_indicator_op_cal_dict[ename][1]+table_alias


    # trend
    trend = {}  # 存放trend 结果

    trend_date=get_trend_where_date(datacopy)
    trenddate=trend_date.replace(' and ', '', 1).lstrip('(t.')

    #日期替换

    trendwhere=re.sub('date_str .*?\)',trenddate,where)


    trendsql = " select " + column + " from " + table + trendwhere + " group by date_str "

    r = requests.get(headers=ck_db['headers'], url=ck_db['host'], params={'query': trendsql})

    if r.status_code == 200 and len(r.text) > 0:
        rtext=r.text.strip('\n')
        rawdata = [ele.split('\t') for ele in rtext.split('\n')]

        trend=get_trend_data(rawdata,datetype)


    itemdict['trend'] = trend


    # 平台分布
    if is_platform_show(datacopy):
        plat = {}

        group_by = " group by date_str"

        if source == 'all':
            column_plat = 'source,'
            platdict = source_dict

            group_by = group_by + ",source "
            order_by = 'order by source'

        elif source == '1' and parentplatform == 'all':  # 1-all-all

            column_plat = get_plat_column(ename)

            platdict = parent_platform_dict

            group_by = group_by + ",_platform "
            order_by = ' order by _platform'

        else:  # 1-1-all
            column_plat = 'platform,'
            platdict = app_dict

            group_by = group_by + ",platform "
            order_by = ' order by platform'

        order_by += ',date_str desc'


        platsql = " select  " + column_plat + column + " from " + table + where + group_by + order_by

        r = requests.get(headers=ck_db['headers'], url=ck_db['host'], params={'query': platsql})

        if r.status_code == 200 and len(r.text) > 0:
            rtext = r.text.strip('\n')
            rawdata = [ele.split('\t') for ele in rtext.split('\n')]

            raw_new_data=[]
            for ele in rawdata:
                ele[1]=float(ele[1])
                raw_new_data.append(ele)

            plat = get_drill_tb_hb(rawdata, platdict, date, datetype)


        itemdict['platform'] = plat


    # 事业部分布
    if is_show_for_user_drill(ename,'bd'):
        bd = {}

        bdid = datacopy['bd_id']
        if bdid =='all':
            bdnamedict = bd_id_dict
        else:
            bdnamedict = cat_name_dict

        column_bd = get_bd_column(ename,bdnamedict)

        group_by = " group by _bd_id,date_str"
        order_by = " order by _bd_id ,date_str desc"

        bdsql = " select  " + column_bd + column + " from " + table + \
                where + group_by + order_by

        r = requests.get(headers=ck_db['headers'], url=ck_db['host'], params={'query': bdsql})

        if r.status_code == 200 and len(r.text) > 0:
            rtext = r.text.strip('\n')
            rawdata = [ele.split('\t') for ele in rtext.split('\n')]

            raw_new_data = []
            for ele in rawdata:
                ele[1] = float(ele[1])
                raw_new_data.append(ele)

            bd = get_drill_tb_hb(rawdata, bdnamedict, date, datetype)

        itemdict['bd'] = bd

    # if source == '1' and parentplatform == 'all':
    #     p_newwhere = " where " + newwhere.replace("source in ('1') and", '')
    #
    #     # 下钻app的时候子类分别去重再加和
    #     '''
    #     column = 'sum(create_parent_uv) as _create_parent_uv,date_str'
    #     column_plat_inner_sql = "select " + "  source,platform,count(distinct cust_id) AS create_parent_uv,date_str " + \
    #                             " from " + ck_tables + p_newwhere + " group by source,platform,date_str"
    #     ck_tables = "(" + column_plat_inner_sql + ")"
    #     p_newwhere = ''
    #     '''
    #
    # else:
    #     p_newwhere = " where " + newwhere


    if is_show_for_user_drill(ename,'customer'):
        #新老客分布
        customer = {}
        if ename=='uv':
            customerdict= new_old_customer_dict

            groupby_new_flag="new_id"
            order_by = " order by new_id," + "date_str desc"
            group_by = " group by date_str" + ",new_id"

            customer_key='uv'
        else:
            customerdict= customer_dict

            groupby_new_flag = new_flag + "_new_flag"
            order_by = " order by " + groupby_new_flag + "," + "date_str desc"
            group_by = " group by date_str" + "," + new_flag + "_new_flag"

            customer_key='customer'

        column_customer=user_indicator_op_cal_dict[ename][0] + "," + column_date

        customersql = "select " + groupby_new_flag + "," + column_customer + " from " + table \
                      + where + group_by + order_by

        r = requests.get(headers=ck_db['headers'], url=ck_db['host'], params={'query': customersql})

        if r.status_code == 200 and len(r.text) > 0:
            rtext = r.text.strip('\n')
            rawdata = [ele.split('\t') for ele in rtext.split('\n')]

            raw_new_data = []
            for ele in rawdata:
                ele[1] = float(ele[1])
                raw_new_data.append(ele)

            customer = get_drill_tb_hb(rawdata, customerdict, date, datetype)

        itemdict[customer_key] = customer


    if is_show_for_user_drill(ename,'quantile'):
        #分位分布-to do
        qualtile={}
        if ename.endswith('sd'):
            table = 'bi_mdata.dm_order_create_detail'
        else:
            table = 'bi_mdata.dm_order_pay_detail'

        groupby=" GROUP BY date_str"
        orderby=" order by date_str desc"
        groupby_custid=" GROUP BY date_str"+",cust_id"

        where_quantile="  AND sale_type = 1 AND product_type != '98' "

        where_quantile = where+where_quantile

        if datetype !='d':
            # where_quantile= re.sub('(t\.)','',where_quantile)
            table=table+" t "

        time_where=get_time_where(datacopy).replace(' and ', '', 1)

        coloumn_quantile="max(t.parent_num) as parentNum_max, sum(t.parent_num)/groupBitmap(toUInt64(t.cust_id)) as parentNum_avg, " \
                        "min(t.parent_num) as parentNum_min, quantileExact(0.5)(t.parent_num) as parentNum_quantile,"+column_date

        table="SELECT cust_id, groupBitmap(toUInt64(parent_id)) as parent_num,"+column_date+" FROM "+table+where_quantile+groupby_custid

        quantitlesql="select "+coloumn_quantile+" from ("+table+") t "+" where "+time_where+groupby+orderby

        r = requests.get(headers=ck_db['headers'], url=ck_db['host'], params={'query': quantitlesql})

        if r.status_code == 200 and len(r.text) > 0:
            rtext = r.text.strip('\n')
            rawdata = [ele.split('\t') for ele in rtext.split('\n')]

            namelist=['最大值','平均值','最小值','中位数']

            length=len(rawdata)

            i = 0
            for parentnum_name in namelist:

                j=0
                while j<length:
                    each_column_list = [[float(ele[i]), ele[-1]] for ele in rawdata if
                                       ele[i] is not None and ele[0] != '0']
                    j+=1
                a=get_overview_tb_hb(each_column_list,parentnum_name,date,datetype)
                qualtile.update(a)

                i+=1
        itemdict['quantile']=qualtile


    sql_drill_data[cname]=itemdict

    return sql_drill_data