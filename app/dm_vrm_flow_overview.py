'''
供应商统一平台-流量概览 (流量、订单只看app平台)
'''


import sys
from sys import path
import json,math,random
import logging
from utils.util import diff
from utils.decorate import logrecord
from utils.db import PyHive

#logger
filepath=path.join(path.dirname(path.dirname(__file__)),"conf","logging.conf")
logging.config.fileConfig(filepath)
supplier_logger = logging.getLogger('jingying_overview')

#目标表结构
flow_source ={
'supplier_num' : '供应商编码',
  'source_id' : '一级渠道id',
  'source_name' : '一级渠道名称',
  'platform_id' : '二级渠道id',
  'platform_name' : '二级渠道名称',
  'pv' : '浏览量',
  'uv' : '访客数',
  'collect_cust_num': '商品收藏人数',
  'cart_cust_num' :'加购人数',
  'pay_cust_num' : '支付用户数',
  'avg_scan_times' : '平均浏览次数（浏览量/访客数）',
  'cart_uv_rate' : '加购率（加购人数/访客数）',
  'pay_uv_rate' : '支付转化率（支付人数/访客数)',
  'data_date':'日期'
}

#统计维度
group_by ='''supplier_num,source_id,platform_id'''
grouping_sets ='''(supplier_num), -- 0 0
(supplier_num,source_id,source_name), -- 1 0
(supplier_num,source_id,source_name,platform_name,platform_id)
'''



def get_test_sql(date_begin,date_end):
    '''

    :param date_begin:
    :param date_end:
    :return:
    '''
    _sql='''WITH flow_detail AS(
SELECT last_arri_supp_no AS supplier_num,
          CASE
              WHEN `source` = '1' THEN 1
              WHEN `source` IN ('2', '3', '4') THEN 2
              ELSE 3
          END AS source_id,
          CASE
              WHEN `source` = '1' AND platform = '0' THEN 1
              WHEN `source` = '1' AND platform IN ('3', '7', '2') THEN 2
              WHEN `source` = '1' AND platform IN ('21') THEN 3
              WHEN platform IN ('105') THEN 4
              WHEN platform IN ('101', '102', '103') THEN 5
              WHEN platform IN ('104') THEN 6
              ELSE 7
          END AS platform_id,
          device_id
FROM dm_dws.mdata_flows_user_realtime
WHERE date_str >= '{date_begin}'
     AND date_str <= '{date_end}'
     AND bd_id_prod != 6
     AND last_arri_supp_no IS NOT NULL
     AND TRIM(last_arri_supp_no)!=''),
collect_detail AS(
SELECT a.*,
    b.last_arri_supp_no AS supplier_num
FROM
     (SELECT product_id,
             cust_id,
             1 AS source_id,
             CASE 
               WHEN platform IN ('1', '2', '3', '4', '5') THEN 1 
               WHEN platform IN ('6', '7', '11') THEN 2 
               WHEN platform IN ('12') THEN 3 
               ELSE 7 
             END AS platform_id
FROM dwd.prod_wish_detail
WHERE trans_date >= '{date_begin}'
        AND trans_date <= '{date_end}'
        AND CAST(product_id AS bigint) > 0
        ) a
JOIN dim.dim_purchase_prod b ON (a.product_id=b.product_id)
WHERE b.last_arri_supp_no IS NOT NULL
     AND TRIM(b.last_arri_supp_no)!=''),
shopcart_detail AS(
SELECT a.*,
          b.last_arri_supp_no AS supplier_num
FROM
     (SELECT 1 AS source_id,
             CASE 
               WHEN from_platform = 0 THEN 1 
               WHEN from_platform IN (3, 7, 2) THEN 2 
               WHEN from_platform IN (21) THEN 3 
               ELSE 7 
             END AS platform_id,
             user_id AS cust_id,
             product_id
      FROM dwd.shopcart_detial
      WHERE trans_date >= '{date_begin}'
        AND trans_date <= '{date_end}'
        AND CAST(product_id AS bigint) > 0
        ) a
JOIN dim.dim_purchase_prod b ON (a.product_id=b.product_id)
WHERE b.last_arri_supp_no IS NOT NULL
     AND TRIM(b.last_arri_supp_no)!=''),
pay_detail AS(
SELECT last_arri_supp_no AS supplier_num,
          CASE
              WHEN `source` = 1 THEN 1
              WHEN `source` IN (2, 3, 4) THEN 2
              ELSE 3
          END AS source_id,
          CASE
              WHEN `source` = 1 AND platform = 0 THEN 1
              WHEN `source` = 1 AND platform IN (1, 2) THEN 2
              WHEN `source` = 1 AND platform IN (4) THEN 3
              WHEN `source` IN (4) THEN 4
              WHEN `source` IN (2) THEN 5
              WHEN `source` IN (3) THEN 6
              ELSE 7
          END AS platform_id,
          cust_id
FROM dm_dws.dm_order_pay_detail
WHERE data_date >= '{date_begin}'
     AND data_date <= '{date_end}'
     AND sale_type = 1
     AND bd_id != 6
     AND last_arri_supp_no IS NOT NULL
     AND TRIM(last_arri_supp_no)!=''),
union_data as(
SELECT supplier_num,
     source_id,
     platform_id,
     1 AS pv,
     device_id as uv ,
     null AS collect_cust,
     null AS cart_cust,
     null AS pay_cust
FROM flow_detail
UNION ALL 
SELECT supplier_num,
       source_id,
       platform_id,
       null AS pv,
       null AS uv,
       cust_id as collect_cust,
       null AS cart_cust_num,
       null AS pay_cust_num
FROM collect_detail
UNION ALL 
SELECT supplier_num,
       source_id,
       platform_id,
       null AS pv,
       null AS uv,
       null AS collect_cust,
       cust_id as cart_cust,
       null AS pay_cust_num
FROM shopcart_detail
UNION ALL 
SELECT supplier_num,
       source_id,
       platform_id,
       null AS pv,
       null AS uv,
       null AS collect_cust,
       null  AS cart_cust,
       cust_id  as pay_cust
FROM pay_detail 
),
union_data_where as (
SELECT supplier_num,
       source_id,
       CASE
          WHEN source_id=1 THEN '主站'
          WHEN source_id=2 THEN '外卖场'
          ELSE '其他'
       END AS source_name,
       platform_id,
       CASE
          WHEN platform_id=1 THEN 'PC'
          WHEN platform_id=2 THEN 'app'
          WHEN platform_id=3 THEN '小程序'
          WHEN platform_id=4 THEN '拼多多'
          WHEN platform_id=5 THEN '天猫'
          WHEN platform_id=6 THEN '抖音'
          ELSE '其他'
       END AS platform_name,
       pv,
       uv,
       collect_cust,
       cart_cust,
       pay_cust
from 
    union_data
where 
    source_id != 3  AND platform_id != 7
)
select 
    supplier_num,
    COALESCE(source_id,0) AS source_id,
    COALESCE(source_name,'全部') AS source_name,
    COALESCE(platform_id,0) AS platform_id,
    COALESCE(platform_name,'全部') AS platform_name,
    count(pv) as pv,
    count(DISTINCT uv) as uv,
    count(DISTINCT collect_cust) as collect_cust_num,
    count(DISTINCT cart_cust) as cart_cust_num,
    count(DISTINCT pay_cust) as pay_cust_num,
    cast(round(count(pv)/count(DISTINCT uv),4) as double) AS `avg_scan_times`,
    cast(round(count(DISTINCT cart_cust)/count(DISTINCT uv),4) as float) AS `cart_uv_rate`,
    cast(round(count(DISTINCT pay_cust)/count(DISTINCT uv),4) as float) AS `pay_uv_rate`,
    '{date_end}' as data_date
from 
    union_data_group
group by {group_by},source_name,platform_name
grouping sets(
{grouping_sets}
)'''
    return _sql.format(date_begin = date_begin,date_end =date_end,group_by =group_by,grouping_sets = grouping_sets)
    pass



@logrecord(supplier_logger)
def do_job(date):

    _flow_source_table = "dm_report.dm_vrm_flow_overview_{}"
    date_begin = date
    date_end = date

    columns = flow_source.keys()
    columns_list = ','.join(columns)
    _where = "supplier_num = '{supplier_num}' and source_id = {source_id} and platform_id = {platform_id}  and  data_date = '{data_date}'"
    _dev_fetch_sql = "select " + columns_list + " from {table}  where {where} "

    hive_db = PyHive()

    for date_type in ['d','wtd','mtd']:
        order_overview_table = _flow_source_table.format(date_type)

        if date_type == 'wtd':
            date_strp = datetime.strptime(date, '%Y-%m-%d')
            date_begin = datetime.strftime(date_strp-timedelta(date_strp.weekday()),'%Y-%m-%d')
        elif date_type == 'mtd':
            date_begin = date[0:-2]+"01"

        test_sql = get_test_sql(date_begin,date_end)
        print(test_sql)
        try :
            test_hive_result = hive_db.get_result_from_db(test_sql)
        except Exception as e:
            print(e.__repr__())
            test_hive_result = []
        finally:
            test_hive_result_length = len(test_hive_result)
            if test_hive_result_length > 0:
                sample_list = random.sample([i for i in range(test_hive_result_length)],min(500, test_hive_result_length))
                for i in sample_list:
                    test_item_hive = dict(zip(columns, test_hive_result[i]))
                    where = _where.format( supplier_num = test_item_hive['supplier_num'],
                                                    source_id = test_item_hive['source_id'],
                                                    platform_id = test_item_hive['platform_id'],
                                                    data_date=test_item_hive['data_date'])
                    dev_fetch_sql = _dev_fetch_sql.format(table = order_overview_table,where = where)

                    dev_item_hive = hive_db.get_result_from_db(dev_fetch_sql)
                    if len(dev_item_hive) > 0:
                        dev_item_hive = dict(zip(columns, dev_item_hive[0]))
                    else:
                        dev_item_hive = {}  # dev table miss

                    diffvalue = diff(test_item_hive, dev_item_hive)
                    print(where)
                    yield where,diffvalue
    hive_db.close_db()

    pass


if __name__ == '__main__':
    date = '2022-04-13'

    do_job(date)