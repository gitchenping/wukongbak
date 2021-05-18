from utils import util
from ._sql import is_platform_show,get_bd_where,get_platform_where,get_time_where,get_shoptype_where,get_eliminate_where,get_drill_tb_hb
from ._sql import get_sql_for_user_analysis_overview_op,get_overview_tb_hb
from utils import tb_hb
from utils.date import get_trend_where_date
import numpy as np
import requests
from resources.map import customer_dict,bd_id_dict,app_dict,source_dict,parent_platform_dict



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

    sql=get_sql_for_user_analysis_overview_op(data,indicator_dict)
    # sql='select 2+2'
    # conn.execute(sql)
    # rawdata=conn.fetchall()
    r=requests.get(headers=ck_db['headers'],url=ck_db['host'],params={'query':sql})

    '''debug
     rawdata=[[360,338,1.14,1.14,'2021-05-12',],
            [None,273,1.09,1.05,'2021-05-11'],
            [198,184,1.1,1.04,'2021-05-05'],
            [216,196,1.06,1.06,'2020-05-12']]
    
    '''
    if r.status_code==200 and len(r.text)>0:
        rawdata=[ele.split('\t') for ele in  r.text.strip('\n').split('\n')]

        #将rawdata转换为narray类型
        raw_data_array=np.array(rawdata)

        i=0
        for indicator_name in indicator_dict.keys():         #循环处理每个指标

            each_indicator=raw_data_array[:,[i,-1]]
            each_indicator_list=each_indicator.tolist()
            each_indicator_list=[[float(ele[0]), ele[1]]  for ele in each_indicator_list if ele[0] is not None and ele[0]!='0']

            each_item_sqldata=get_overview_tb_hb(each_indicator_list, test_indicator_dict[indicator_name], date, datetype)

            sqldata.update(each_item_sqldata)

            i+=1
            pass

    return sqldata


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



def sql_user_analysis_drill_op(datacopy, ck_tables, test_indicator_dict,conn=None):
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
        bdnamedict = bd_id_dict
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
                platdict = source_dict

                group_by = group_by + "source "
                order_by = 'order by source'
            elif source=='1' and parentplatform=='all':     #1-all-all
                column_plat ="(case when platform in ('0') then '4' " \
                             "when source='1' and platform in ('12','20') then '3' " \
                             "when source='1' and platform in ('1','2') then '1' else '2' end) as _platform,"

                platdict = parent_platform_dict

                group_by = group_by + "_platform "
                order_by = ' order by _platform'

            else:                                     #1-1-all
                column_plat = 'platform,'
                platdict = app_dict

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
        #新老客分布
        customer = {}
        customerdict= customer_dict
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

        #分位分布-to do
        if zhibiao in ['daycount_ratio_sd','daycount_ratio_zf']:                            #收订下单频次\支付下单频次有

            pass


    return sql_data