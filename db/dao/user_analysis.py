from utils import util
from ._sql import is_platform_show,get_bd_where,get_platform_where,get_time_where,get_shoptype_where,get_eliminate_where,get_drill_tb_hb
from ._sql import get_overview_tb_hb
from ._sql import get_where_for_analysis_overview_op,is_show_for_user_drill,get_plat_column,get_bd_column
from utils.tb_hb import get_tb_hb_key_dict
from utils.date import get_trend_where_date,get_trend_data
import numpy as np
import requests
import re
from resources.map import customer_dict,new_old_customer_dict,bd_id_dict, \
    bd_id_cat,app_dict,source_dict,parent_platform_dict,user_drill_cat_name_dict,parent_platform_cat


#用户分支优化各指标计算逻辑
user_indicator_op_cal_dict={
    "new_uv":['uniqExactMerge(device_id_state) as new_uv','mdata_flows_user_realtime_day_all'],  #"新访UV"
    "new_uv_ratio":['new_uv / uv *100 as new_uv_ratio','mdata_flows_user_realtime_day_all'],  #"新访uv占比"
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

user_indicator_op_realtime_cal_dict={
    "new_uv":['count(distinct if(new_id=1, device_id, null)) AS new_uv','mdata_flows_user_realtime_all'],  #"新访UV"
    "new_uv_ratio":['round(count(distinct if(new_id=1, device_id, null))/count(distinct device_id)*100,0) AS new_uv_ratio','mdata_flows_user_realtime_all'],  #"新访uv占比"
    # "register_number":['count(distinct cust_id) as register_number','mdata_customer_new_all'] ,    #"新增注册用户"
    "new_create_parent_uv_sd":["count(distinct if(new_id='1' AND data_type='1', cust_id, null)) AS new_create_parent_uv_sd",'kpi_order_info_all_v2'],  #"新增收订用户"
    "new_create_parent_uv_zf":["count(distinct if(new_id='1' AND data_type='2', cust_id, null)) AS new_create_parent_uv_zf",'kpi_order_info_all_v2'],             #"新增支付用户"
    "new_create_parent_uv_ck": ["count(distinct if(new_id='1' AND data_type='3', cust_id, null)) AS new_create_parent_uv_ck",'kpi_order_info_all_v2'],             #"新增出库用户"
    "uv":['count(distinct device_id) AS uv','mdata_flows_user_realtime_all'] ,        #"活跃UV"
    "create_parent_uv_sd":["count(distinct  if(data_type='1', cust_id, null)) AS create_parent_uv_sd",'kpi_order_info_all_v2'],#"收订用户"
    "create_parent_uv_zf":["count(distinct  if(data_type='2', cust_id, null)) AS create_parent_uv_zf",'kpi_order_info_all_v2'],#'"支付用户"
    "create_parent_uv_ck":["count(distinct  if(data_type='3', cust_id, null)) AS create_parent_uv_ck",'kpi_order_info_all_v2'],#"出库用户",
    # "daycount_ratio_sd":['groupBitmapMerge(parent_id_state)/groupBitmapMerge(cust_id_state) as daycount_ratio_sd','dm_order_create_day'],#"收订下单频次",
    # "daycount_ratio_zf":['groupBitmapMerge(parent_id_state)/groupBitmapMerge(cust_id_state) as daycount_ratio_zf','dm_order_pay_day'] #"支付下单频次"
}

#根据new_uv uv获取占比
def get_new_uv_ratio(new_uv,uv):
    result = {}
    result['新访UV占比'] = {}
    newuv = new_uv['新访UV']
    uv = uv['活跃UV']

    for item, itemvalue in newuv.items():
        if item == 'trend':
            newuv_trend = itemvalue
            uv_trend = uv[item]

            trend = {}
            for key in newuv_trend.keys():
                try:
                    ratio = round(newuv_trend[key] / uv_trend[key] * 100, 2)
                    trend[key] = ratio
                except Exception:
                    continue
            if trend != {}:
                result['新访UV占比'].update({'trend': trend})

        if item == "platform":
            newuv_platform = itemvalue
            uv_platform = uv[item]

            platform = {}
            for key, newuvvalue in newuv_platform.items():
                platform[key] = {}

                uvvalue = uv_platform[key]

                value = 0
                for vthk, vthv in newuvvalue.items():
                    try:
                        if vthk == "value":
                            value = round(newuvvalue[vthk] / uvvalue[vthk] * 100, 2)
                            platform[key].update({vthk: value})

                        else:
                            a = newuvvalue['value'] / (1 + newuvvalue[vthk] / 100)

                            b = uvvalue['value'] / (1 + uvvalue[vthk] / 100)

                            hb_tb_ratio_value = round(a / b * 100, 2)
                            platform[key].update({vthk: round((value / hb_tb_ratio_value - 1) * 100, 2)})
                    except Exception:
                        continue
            if platform != {}:
                result['新访UV占比'].update({'platform': platform})

    return result


def get_sql_for_user_analysis_overview_op(data,indicator,user_indicator_cal_dict,method=1):
    '''用户分析优化'''

    date_type=data['date_type']
    if date_type=='d':
        new_flag='day'
        column_date = 'date_str'
    elif date_type=='w':
        new_flag = 'week'
        column_date='toStartOfWeek(toDate(date_str), 1) as date_str'
    elif date_type=='m':
        new_flag = 'month'
        column_date='toStartOfMonth(toDate(date_str)) as date_str'
    elif date_type=='q':
        new_flag='quarter'
        column_date = 'toStartOfQuarter(toDate(date_str)) as date_str'
    else:                               #小时
        column_date = 'toDate(date_str) as date_str'


    # where=" where bd_id IN (1,4,9,15,16) AND platform IN (1,2) and shop_type=1 and  date_str IN ('2021-05-12','2020-05-12','2021-05-05','2021-05-11') "
    groupby = "  group by date_str "
    orderby = " order by date_str desc"


    #根据指标拼接sql，一个

    sqlset_dict={}

    for ename,cname in indicator.items():

        if ename=='new_uv_ratio':
            continue
        where=get_where_for_analysis_overview_op(data,ename)

        column = user_indicator_cal_dict[ename][0]+" ,"+column_date
        table = 'bi_mdata.'+user_indicator_cal_dict[ename][1]

        sql="select " + column + " from " + table + " t "+where + groupby + orderby
        sqlset_dict[ename]=sql

    if method==1:
        #新访UV占比
        if sqlset_dict.__contains__('uv') and sqlset_dict.__contains__('new_uv'):
            uv_ratio_sql="select "+ user_indicator_cal_dict['new_uv']+",t1.date_str  from ("+sqlset_dict['new_uv']+") t1 "+" left join ("+sqlset_dict['uv']+") t2 on t1.date_str=t2.date_str "+orderby

            sqlset_dict['new_uv_ratio']=uv_ratio_sql

            return sqlset_dict
    else:
        #第二种方法写作一个sql,使用full join
        i=1
        sql_list=[]
        outer_column=[]
        for ename, sql in sqlset_dict.items():
            # outer_column.append("t"+str(i)+"."+ename)
            outer_column.append(ename)
            sql_list.append("("+sql+") t" +str(i))
            i+=1

        length=len(sql_list)
        sql_list = [sql_list[0]] + [sql_list[i] + " on t1" + ".date_str=t" + str(i + 1) + ".date_str" for i in
                                    range(1, length)]
        sql = ' full join '.join(sql_list)

        #最后算new_uv_ratio
        new_uv_ratio_column=''
        if indicator.__contains__('new_uv_ratio') :
            uv_index=outer_column.index('uv')+1
            new_uv_index=outer_column.index('new_uv') + 1

            new_uv_ratio_column="t"+str(new_uv_index)+".new_uv" +" / "+"t"+str(uv_index)+".uv *100" +" as new_uv_ratio"

        outer_column_t=["t"+str(i+1)+"."+outer_column[i] for i in range(0,len(outer_column))]  #列名
        all_column_datestr_list=["t"+str(i+1)+"."+"date_str" for i in range(0,len(outer_column))] #时间

        if new_uv_ratio_column != '':
            outer_column.append('new_uv_ratio')
            outer_column_t.append(new_uv_ratio_column)
            all_column_datestr_list.append('t'+str(new_uv_index)+".date_str")       #与new_uv 时间保持一致

        outer_column_str=",".join(outer_column_t)
        all_column_datestr=','.join(all_column_datestr_list)

        final_sql="select "+ outer_column_str+","+all_column_datestr+" from "+sql


        return final_sql,outer_column

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


def sql_user_analysis_overview_op(data, test_indicator_dict, ck_conn=None,is_novalue_key_show=False,defaultvalue='--'):
    '''

    :param data: 筛选条件
    :param test_indicator_dict: 所有的测试指标字典
    :param ck_conn: 自定义ck连接
    :param is_novalue_key_show: 指标没有数值的话是否参与比较
    :param defaultvalue: 缺失的时候默认值
    :return: 来自数据库的数据
    '''
    indicator_dict=dict(test_indicator_dict)
    date_hour = data['date'].split(' ')       #对于时的情况，传值形式为2021-05-21 13

    date=date_hour[0]
    datetype = data['date_type']

    # 根据datetype，获取同环比键
    tbhb_keydict = get_tb_hb_key_dict(datetype)

    sqldata={}
    method=2
    user_indicator_cal_dict=user_indicator_op_cal_dict
    if datetype.startswith('h'):
        user_indicator_cal_dict=user_indicator_op_realtime_cal_dict

    sql_dict_str,column_list=get_sql_for_user_analysis_overview_op(data,indicator_dict,user_indicator_cal_dict,method)

    if method==1:              #两种处理方法：1、一个指标一个sql处理，2,所有指标sql汇总一个大sql
        #依次处理每个指标
        for ename,sql in sql_dict_str.items():
            each_item_sqldata={}
            cname=test_indicator_dict[ename]

            # r=requests.get(headers=ck_db['headers'],url=ck_db['host'],params={'query':sql})
            rawdata=ck_conn.cmd_linux(sql)

            if len(rawdata)>0:

                each_item_sqldata[cname] = get_overview_tb_hb(rawdata,tbhb_keydict,ename, date,datetype,misskeyshow=False)
                sqldata.update(each_item_sqldata)
            else:
                sqldata[cname]={}              #和api 保持一致
    else:
        rawdata = ck_conn.ck_get(sql_dict_str)

        length = len(rawdata)
        if length > 0:
            length = len(rawdata[0])
            rawdata_array=np.array(rawdata)

            for i in range(length // 2):
                #依次取i列和i+length/2列
                temp = rawdata_array[:,[i,i + length // 2]]
                templist = temp.tolist()
                ename = column_list[i]
                cname = test_indicator_dict[ename]

                sql_result= get_overview_tb_hb(templist, tbhb_keydict, date, datetype, misskeyshow=False)
                if sql_result  == {}:
                    if is_novalue_key_show:
                        sqldata[cname] = {}
                else:
                    sqldata[cname]=sql_result

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


def sql_user_analysis_drill_op(datacopy,conn,ename='',cname=''):
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

    groupby=' group by date_str'
    if date_type.startswith('h'):
        column_date=' hour_str'
        groupby = ' group by hour_str'
        groupby_new_flag = "new_id"
    elif date_type == 'd':
        column_date = 'date_str'
        groupby_new_flag =  "day_new_flag"
    elif date_type == 'w':
        column_date = 'toStartOfWeek(toDate(date_str), 1) as date_str'
        groupby_new_flag =  "week_new_flag"
    elif date_type == 'm':
        column_date = 'toStartOfMonth(toDate(date_str)) as date_str'
        groupby_new_flag =  "month_new_flag"
    else:
        column_date = 'toStartOfQuarter(toDate(date_str)) as date_str'
        groupby_new_flag =  "quarter_new_flag"


    source = datacopy['source']
    parentplatform = datacopy['parent_platform']

    date = datacopy['date']
    datetype=datacopy['date_type']

    table_alias = ''
    if datetype not in ['d', 'h']:
        table_alias = ' t'

    user_indicator_cal_dict = user_indicator_op_cal_dict
    if datetype.startswith('h'):
        user_indicator_cal_dict = user_indicator_op_realtime_cal_dict
    table = "bi_mdata." + user_indicator_cal_dict[ename][1] + table_alias

    # 根据datetype，获取可能的同环比键
    tbhb_keydict = get_tb_hb_key_dict(datetype)

    where=get_where_for_analysis_overview_op(datacopy,ename)

    column = user_indicator_cal_dict[ename][0] + "," + column_date
    if ename == "new_uv_ratio":

        new_uv_where = get_where_for_analysis_overview_op(datacopy,'new_uv')
        uv_where = get_where_for_analysis_overview_op(datacopy,'uv')

        column = column.replace('date_str',' t1.date_str')


        table=" (select "+user_indicator_cal_dict['new_uv'][0] +","+column_date + " from bi_mdata."+ user_indicator_cal_dict['new_uv'][1] + new_uv_where+groupby+" ) t1 left join "+ \
            "( select "+user_indicator_cal_dict['uv'][0] +","+column_date+" from bi_mdata."+ user_indicator_cal_dict['uv'][1]+uv_where +groupby+") t2 on t1.date_str=t2.date_str "

        groupby = groupby.replace('date_str', ' t1.date_str')

        '''
        new_uv_info = sql_user_analysis_drill_op(datacopy, conn, ename='new_uv', cname='新访UV')
        uv_info = sql_user_analysis_drill_op(datacopy, conn, ename='uv', cname='活跃UV')
        
        new_uv_ratio = {}
        if new_uv_info['新访UV'] != {} and uv_info['活跃UV'] != {}:
            # uv_info['活跃UV'].pop('uv')

            new_uv_ratio = get_new_uv_ratio(new_uv_info, uv_info)
            if new_uv_ratio['新访UV占比'] == {}:
                new_uv_ratio['新访UV占比']['trend'] = {}
                if is_platform_show(datacopy):
                    new_uv_ratio['新访UV占比']['platform'] = {}

        return new_uv_ratio
        '''



    # trend
    trend = {}  # 存放trend 结果

    trend_date=get_trend_where_date(datacopy)
    trenddate = trend_date.replace(' and ', '', 1).lstrip('(t.')

    if date_type.startswith('h'):
        trendwhere=" where "+trenddate
    else:
        #日期替换
        trendwhere=re.sub('date_str .*?\)',trenddate,where)


    trendtable = table
    trendgroupby = groupby
    if ename == "new_uv_ratio":
        trendtable = re.sub('date_str in .*?\)',trenddate,table)
        trendwhere=''
        trendgroupby=''


    trendsql = " select " + column + " from " + trendtable + trendwhere + trendgroupby

    rawdata=conn.ck_get(trendsql)
    trend=get_trend_data(rawdata,datetype)

    itemdict['trend'] = trend


    #下面的分布中，小时 时间字段不再使用hour_str
    if  date_type.startswith('h'):
        column=column.replace('hour_str', 'date_str', 1)

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

            is_str = False
            if datetype.startswith('h'):
                is_str = True

            column_plat = get_plat_column(ename,parent_platform_cat,is_str)

            platdict = parent_platform_dict

            group_by = group_by + ",_platform "
            order_by = ' order by _platform'

        else:  # 1-1-all
            column_plat = 'platform,'
            platdict = app_dict

            group_by = group_by + ",platform "
            order_by = ' order by platform'

        order_by += ',date_str desc'

        if ename == 'new_uv_ratio':
            table = " (select " +column_plat+ user_indicator_cal_dict['new_uv'][0] + "," + column_date + " from bi_mdata." + \
                    user_indicator_cal_dict['new_uv'][1] + new_uv_where + group_by + " ) t1 left join " + \
                    "( select " +column_plat+user_indicator_cal_dict['uv'][0] + "," + column_date + " from bi_mdata." + \
                    user_indicator_cal_dict['uv'][1] + uv_where + group_by + ") t2 on t1.date_str=t2.date_str "

            where=''
            group_by=''
            order_by=''

        platsql = " select  " + column_plat + column + " from " + table + where + group_by + order_by
        rawdata = conn.ck_get(platsql)

        plat = get_drill_tb_hb(rawdata, tbhb_keydict,platdict, date, datetype,misskeyshow=False)


        itemdict['platform'] = plat


    # 事业部分布
    if is_show_for_user_drill(ename,'bd'):
        bd = {}

        bdid = datacopy['bd_id']
        if bdid =='all':
            bdnamedict = bd_id_dict
        else:
            bdnamedict = user_drill_cat_name_dict

        is_str = False
        if datetype.startswith('h'):
            if ename in ['uv', 'new_uv']:
                is_str = True

        column_bd = get_bd_column(bdnamedict,is_str)

        group_by = " group by _bd_id,date_str"
        order_by = " order by _bd_id ,date_str desc"

        bdsql = " select  " + column_bd + column + " from " + table + \
                where + group_by + order_by

        rawdata=[]
        if bdid=='all':
            rawdata = conn.ck_get(bdsql)

        else:

            rawdata=conn.cmd_linux(bdsql)

        bd = get_drill_tb_hb(rawdata,tbhb_keydict, bdnamedict, date, datetype,misskeyshow=False)

        itemdict['bd'] = bd


    # 新老（访）客分布
    if is_show_for_user_drill(ename,'customer'):

        customer = {}
        if ename=='uv':                          #新老访客
            customerdict= new_old_customer_dict
            customer_key='uv'
        else:
            customerdict= customer_dict
            customer_key='customer'

        order_by = " order by " + groupby_new_flag + "," + "date_str desc"
        group_by = " group by date_str" + "," + groupby_new_flag


        customersql = "select " + groupby_new_flag + "," + column + " from " + table \
                      + where + group_by + order_by

        rawdata = conn.ck_get(customersql)
        customer = get_drill_tb_hb(rawdata,tbhb_keydict, customerdict, date, datetype,misskeyshow=False)

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

        rawdata = conn.ck_get(quantitlesql)

        if  len(rawdata) > 0:

            namelist=['最大值','平均值','最小值','中位数']

            length=len(rawdata)

            i = 0
            for parentnum_name in namelist:

                j=0
                while j<length:
                    each_column_list = [[float(ele[i]), ele[-1]] for ele in rawdata if
                                       ele[i] is not None and ele[0] != '0']
                    j+=1
                a=get_overview_tb_hb(each_column_list,parentnum_name,date,datetype,misskeyshow=False)
                qualtile.update(a)

                i+=1
        itemdict['quantile']=qualtile


    sql_drill_data[cname]=itemdict

    return sql_drill_data