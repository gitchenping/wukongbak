'''
实时概览-数据趋势
prd:https://axhub.im/pro/fbf2b2c650dd204a/#g=1&p=实时趋势
开发逻辑：http://wiki.dangdang.cn/pages/viewpage.action?pageId=29220738
'''

import os
from os import path
import logging
import json,math,random
from decimal import Decimal
from utils.db import PyCK,PyMysql
from utils.util import diff
from utils.decorate import logrecord

#logger
filepath=path.join(path.dirname(path.dirname(__file__)),"conf","logging.conf")
logging.config.fileConfig(filepath)
supplier_logger=logging.getLogger('jingying_overview')


#目标信息
table_info = {
    'table':'supplier.supp_dm_vrm_flow_pay_overview_hour',
    'columns':{
        'supplier_num': '供应商编码',
        'source_id': '一级渠道编码id: 0：全部 1 :主站 2：外卖场 3:其他',
        'source_name': '一级渠道名称: 0：全部 1 :主站 2：外卖场 3:其他',
        'platform_id': '二级渠道id: 0：全部 1：pc 2:app 3:小程序 4:拼多多 5：天猫 6：抖音 7：其他',
        'platform_name': '二级渠道名称: 0：全部 1：pc 2:app 3:小程序 4:拼多多 5：天猫 6：抖音 7：其他',
        'pv': '浏览量',
        'uv': '访客数',
        'pay_cust_num': '支付人数',
        'pay_order_num': '支付单量',
        'pay_product_num': '商品销量',
        'prod_sale_amt': '支付实洋',
        'pay_uv_rate': '支付转化率（支付人数/访客数）',
        'pay_fixed_amt_atv': '实付客单价（支付实洋/支付人数）',
        'data_date': '日期',
        'data_hour': '小时'
    },
    'where':" where supplier_num = '{supplier_num}' and source_id = {source_id} and platform_id = {platform_id} "
            "and data_date = '{data_date}' and data_hour = '{data_hour}'",
    'src_con':PyCK(),
    'dst_con':PyMysql(host='10.4.40.149', port=9306, user='root',password="root123", database='supplier')
}


#渠道维度
#维度
flow_weidu_dict = {
    'flow_source_id':'cast(source as int)',
    'flow_source_name':"'主站'",
    'flow_platform_id':'''CASE  
        WHEN from_platform = '0' THEN 1 
        WHEN from_platform IN ('3', '7', '2') THEN 2  
        WHEN from_platform ='21' THEN 3 
        ELSE 7  END ''',
    'flow_platform_name':'''CASE  
        WHEN from_platform = '0' THEN 'PC'  
        WHEN from_platform IN ('3', '7', '2') THEN 'app'  
        WHEN from_platform IN ('21') THEN '小程序'  
        ELSE '其他' end '''
}

pay_weidu_dict = {
#1、一级渠道id
'pay_source_id' :'''case when source='1' then 1 
     when  source in ('2','3','4')  then 2 
     else 3 end ''',
#2、一级渠道名称
'pay_source_name' : '''case when source='1' then '主站' 
     when  source in ('2','3','4')  then '外卖场' 
     else '其他' end ''',
#3、二级渠道id
'pay_platform_id' : '''case when source='1' and platform='0' then 1 
     when source = '1' and platform in ('3','7','2') then 2
     when source = '1' and platform in ('21') then 3
     when platform in ('105') then 4
     when platform in ('101','102','103') then 5
     when platform in ('104') then 6
     else 7 end ''',
#4、二级渠道名称
'pay_platform_name' : '''case when source='1' and platform='0' then 'PC' 
     when  source='1' and platform in('3','7','2') then 'app' 
     when  source='1' and platform in ('21') then '小程序' 
     when platform in ('105') then '拼多多'
     when platform in ('101','102','103') then '天猫'
     when platform in ('104') then '抖音'
     else '其他' end '''
}

#筛选条件
where= {

    'flow_where':'''  source in('1') and from_platform in('0','2','3','7','21') 
                and bd_id_prod <> 6
                and last_arri_supp_no<> 'null' 
                and last_arri_supp_no is not null 
                and last_arri_supp_no <>'' ''',
    'pay_where':'''  source in('1','2','3','4') and platform in('0','3','7','2','21','105','104','101','102','103')
                and last_arri_supp_no <> 'null' 
                and last_arri_supp_no is not null 
                and last_arri_supp_no<>'' ''',
    'filter_other':" source_id != 3 and platform_id != 7"
}

def get_test_sql(date_str,hour_str):
    '''

    :param date_str:
    :param hour_str:
    :return:
    '''
    sql = '''
    select 
    last_arri_supp_no, 
    source_id, 
    source_name, 
    platform_id, 
    platform_name, 
    sum(t.pv) as pv, 
    sum(t.uv) as uv, 
    sum(t.pay_cust_num) as pay_cust_num, 
    sum(t.pay_order_num) as pay_order_num, 
    sum(t.pay_product_num) as pay_product_num, 
    round(sum(t.prod_sale_amt),2) as prod_sale_amt, 
    case when sum(t.uv)==0 then 0.00 else round(sum(t.pay_cust_num)/sum(t.uv),2) end as pay_uv_rate, 
    case when sum(t.pay_cust_num)==0 then 0.00 else round(sum(t.prod_sale_amt)/sum(t.pay_cust_num),2) end as pay_fixed_amt_atv, 
    toDate(data_date) as data_date,
    '{hour_str}' as data_hour
    from (   
    select 
        last_arri_supp_no,
        source_id,
        source_name,
        platform_id,
        platform_name,
        count(*) as pv, 
        count(distinct device_id) as uv, 
        0 as pay_cust_num, 
        0 as pay_order_num, 
        0 as pay_product_num, 
        0 as prod_sale_amt, 
        '{date_str}' as data_date 
    from (
        select 
            last_arri_supp_no,
            {flow_source_id} as source_id,
            {flow_source_name} as source_name,
            {flow_platform_id} as platform_id,
            {flow_platform_name} as platform_name,
            device_id
            from 
                bi_mdata.mdata_flows_user_realtime_all 
            where date_str = '{date_str}' 
                and hour_str <= '{hour_str}'
                and {flow_where}
        ) t 
    group by last_arri_supp_no ,source_id,source_name,platform_id,platform_name 
    union all 
    select 
        last_arri_supp_no,
        source_id,
        source_name,
        platform_id,
        platform_name,
        count(*) as pv, 
        count(distinct device_id) as uv, 
        0 as pay_cust_num, 
        0 as pay_order_num, 
        0 as pay_product_num, 
        0 as prod_sale_amt, 
        '{date_str}' as data_date 
    from (
        select 
            last_arri_supp_no,
            {flow_source_id} as source_id,
            {flow_source_name} as source_name,
            0 as platform_id,
            '全部' as platform_name,
            device_id
            from 
                bi_mdata.mdata_flows_user_realtime_all 
            where date_str = '{date_str}' 
                and hour_str <= '{hour_str}'
                and {flow_where}
        ) t 
    group by last_arri_supp_no ,source_id,source_name,platform_id,platform_name 
    union all 
    select 
        last_arri_supp_no,
        source_id,
        source_name,
        platform_id,
        platform_name,
        count(*) as pv, 
        count(distinct device_id) as uv, 
        0 as pay_cust_num, 
        0 as pay_order_num, 
        0 as pay_product_num, 
        0 as prod_sale_amt, 
        '{date_str}' as data_date 
    from (
        select 
            last_arri_supp_no,
            0 as source_id,
            '全部' as source_name,
            0 as platform_id,
            '全部' as platform_name,
            device_id
            from 
                bi_mdata.mdata_flows_user_realtime_all 
            where date_str = '{date_str}' 
                and hour_str <= '{hour_str}'
                and {flow_where}
        ) t 
    group by  last_arri_supp_no ,source_id,source_name,platform_id,platform_name 
   union all
    select 
        last_arri_supp_no,
        source_id,
        source_name,
        platform_id,
        platform_name,
        0 as pv, 
        0 as uv,  
        count(distinct t.cust_id) as pay_cust_num, 
        count(distinct t.order_id) as pay_order_num, 
        sum(t.order_quantity) as pay_product_num, 
        sum(case when order_status=-1000 then bargin_price*order_quantity*-1 else bargin_price*order_quantity end) as prod_sale_amt, 
        '{date_str}' as data_date 
    from (
        select
            last_arri_supp_no, 
            {pay_source_id} as source_id,
            {pay_source_name} as source_name,
            {pay_platform_id} as platform_id,
            {pay_platform_name} as platform_name,
            cust_id,
            order_id,
            order_status,
            bargin_price,
            order_quantity
         from 
                bi_mdata.realtime_order_items_detail_pay 
         where date_str = '{date_str}' 
                and hour_str <= '{hour_str}' 
                and {pay_where} 
    ) t 
     group by  
         last_arri_supp_no ,source_id,source_name,platform_id,platform_name 
    union all 
     select 
        last_arri_supp_no,
        source_id,
        source_name,
        platform_id,
        platform_name,
        0 as pv, 
        0 as uv,  
        count(distinct t.cust_id) as pay_cust_num, 
        count(distinct t.order_id) as pay_order_num, 
        sum(t.order_quantity) as pay_product_num, 
        sum(case when order_status=-1000 then bargin_price*order_quantity*-1 else bargin_price*order_quantity end) as prod_sale_amt, 
        '{date_str}' as data_date 
    from (
        select
            last_arri_supp_no, 
            {pay_source_id} as source_id,
            {pay_source_name} as source_name,
            0 as platform_id,
            '全部' as platform_name,
            cust_id,
            order_id,
            order_status,
            bargin_price,
            order_quantity
         from 
                bi_mdata.realtime_order_items_detail_pay 
         where date_str = '{date_str}' 
                and hour_str <= '{hour_str}' 
                and {pay_where} 
         ) t 
     group by  
         last_arri_supp_no ,source_id,source_name,platform_id,platform_name  
    union all 
    select 
        last_arri_supp_no,
        source_id,
        source_name,
        platform_id,
        platform_name,
        0 as pv, 
        0 as uv,  
        count(distinct t.cust_id) as pay_cust_num, 
        count(distinct t.order_id) as pay_order_num, 
        sum(t.order_quantity) as pay_product_num, 
        sum(case when order_status=-1000 then bargin_price*order_quantity*-1 else bargin_price*order_quantity end) as prod_sale_amt, 
        '{date_str}' as data_date 
    from (
        select
            last_arri_supp_no, 
            0 as source_id,
            '全部' as source_name,
            0 as platform_id,
            '全部' as platform_name,
            cust_id,
            order_id,
            order_status,
            bargin_price,
            order_quantity
         from 
                bi_mdata.realtime_order_items_detail_pay 
         where date_str = '{date_str}' 
                and hour_str <= '{hour_str}' 
                and {pay_where} 
         ) t 
     group by  
         last_arri_supp_no ,source_id,source_name,platform_id,platform_name 
    )t 
     group by  
         last_arri_supp_no ,source_id,source_name,platform_id,platform_name ,data_date     
    '''
    return sql.format(**flow_weidu_dict,**pay_weidu_dict,**where,date_str = date_str,hour_str = hour_str)
    pass




@logrecord(supplier_logger)
def do_job(date_str,last_change_date):

    table = table_info['table']
    columns_keys = table_info['columns'].keys()
    columns = ','.join(columns_keys)

    _where = table_info['where']

    src_hd = table_info['src_con']
    dst_hd = table_info['dst_con']

    _dev_fetch_sql = "select {columns} from {table} ".format(columns = columns,table = table)

    for date_type in ['today'][0:]:

        test_sql = get_test_sql(date_str,last_change_date)
        print(test_sql)
        try :
            test_hive_result = src_hd.get_result_from_db(test_sql)
        except Exception as e:
            print(e.__repr__())
            test_hive_result = []
        finally:
            test_hive_result_length = len(test_hive_result)
            if test_hive_result_length > 0:
                sample_list = random.sample([i for i in range(test_hive_result_length)],min(500, test_hive_result_length))
                for i in sample_list:
                    test_item_hive = dict(zip(columns_keys, test_hive_result[i]))
                    where = _where.format(**test_item_hive)
                    dev_fetch_sql = _dev_fetch_sql + where

                    dev_item_hive = dst_hd.get_result_from_db(dev_fetch_sql)

                    if len(dev_item_hive) > 0:
                        dev_item_hive = dict(zip(columns_keys, dev_item_hive[0]))
                        dev_item_hive_change(dev_item_hive)
                    else:
                        dev_item_hive = {}  # dev table miss

                    diffvalue = diff(test_item_hive, dev_item_hive)
                    #print(where)
                    yield where,diffvalue
    src_hd.close_db()
    dst_hd.close_db()


    pass


if __name__ == '__main__':
    date_str = '2022-05-16'
    hour_str = '16'
    do_job(date_str,hour_str)