'''
供应商数据中心:交易分析-交易概况
'''

import os
import sys
from os import path
import logging
import json,math,random,datetime
from utils.decorate import logrecord
from utils.util import diff
from utils.db import PyHive

#logger
filepath=path.join(path.dirname(path.dirname(__file__)),"conf","logging.conf")
logging.config.fileConfig(filepath)
supplier_logger=logging.getLogger('jingying_overview')

#目标表结构
order_overview ={
'supplier_num' : '供应商编码',
  'source_id' : '一级渠道id',
  'source_name' : '一级渠道名称',
  'platform_id' : '二级渠道id',
  'platform_name' : '二级渠道名称',
  'prod_sale_fixed_amt' : '支付码洋',
  'prod_sale_amt' : '支付实洋',
  'pay_cust_num' : '支付用户数',
  'pay_order_num' : '支付订单数',
  'pay_product_num' : '支付商品件数',
  'cancel_order_num' : '取消订单数',
  'prod_sale_amt_rate' : '支付实洋占比(每个渠道支付实洋占供应商总支付实洋的比例)',
  'pay_atv' : '支付客单价（支付实洋/支付用户数）',
  'cancel_order_rate' : '取消率（取消单量/支付单量*100%）',
  'data_date':'日期'
}


#维度
weidu_dict = {
#1、一级渠道id
'source_id' :'''case when source='1' then 1 
     when  source in ('2','3','4')  then 2 
     else 3 
end as source_id''',
#2、一级渠道名称
'source_name' : '''case when source='1' then '主站' 
     when  source in ('2','3','4')  then '外卖场' 
     else '其他' 
end as source_name''',
#3、二级渠道id
'platform_id' : '''case when source=1 and from_platform='0' then 1 
     when source = 1 and from_platform in ('3','7','2') then 2
     when source = 1 and from_platform in ('21') then 3
     when from_platform in ('105') then 4
     when from_platform in ('101','102','103') then 5
     when from_platform in ('104') then 6
     else 7 
end as platform_id''',
#4、二级渠道名称
'platform_name' : '''case when source=1 and from_platform='0' then 'PC' 
     when  source=1 and from_platform in('3','7','2') then 'app' 
     when  source=1 and from_platform in ('21') then '小程序' 
     when from_platform in ('105') then '拼多多'
     when from_platform in ('101','102','103') then '天猫'
     when from_platform in ('104') then '抖音'
     else '其他'
end as platform_name'''
}


def get_test_sql(date_begin,date_end):
    '''

    :param date_str:
    :return:
    '''

    pay_sql = '''with pay_base as(
        select 
        supplier_num,
        {source_id},
        {source_name},
        {platform_id},
        {platform_name},
        prod_sale_fixed_amt,
        prod_sale_amt,
        cust_id,
        parent_id,
        prod_sale_qty 
        from dm_dws.dm_order_pay_detail 
        where data_date >='{date_begin}' and data_date <= '{date_end}' 
        and bd_id !=6 and sale_type=1 and supplier_num is not null and trim(supplier_num) != ''
    ),
    '''.format(**weidu_dict,date_begin = date_begin,date_end= date_end)
    cancel_sql = '''cancel_base as(
        select 
        supplier_num,
        {source_id},
        {source_name},
        {platform_id},
        {platform_name},
        parent_id
        from dm_dws.dm_order_cancel_detail 
        where data_date >='{date_begin}' and data_date <= '{date_end}' 
        and bd_id !=6 and supplier_num is not null and trim(supplier_num) != ''
    ),
    '''.format(**weidu_dict,date_begin = date_begin,date_end= date_end)

    t = '''union_table as(select
   supplier_num,
   source_id,
   source_name,
   platform_id,
   platform_name,
   sum(prod_sale_fixed_amt) prod_sale_fixed_amt,
   sum(prod_sale_amt) prod_sale_amt,
   count(distinct cust_id) pay_cust_num,
   count(distinct parent_id) pay_order_num,
   sum(prod_sale_qty) pay_product_num,
   count(distinct parent_id_cancel) cancel_order_num
   from (
        select 
          supplier_num,
          source_id,
          source_name,
          platform_id,
          platform_name,
          prod_sale_fixed_amt,
          prod_sale_amt,
          cust_id,
          parent_id,
          prod_sale_qty,
          null as parent_id_cancel
        from
          pay_base where source_id != 3 and platform_id != 7
        union all 
        select 
          supplier_num,
          source_id,
          source_name,
          platform_id,
          platform_name,
          0 prod_sale_fixed_amt,
          0 prod_sale_amt,
          null as cust_id,
          null as parent_id,
          0 as prod_sale_qty,
          parent_id as parent_id_cancel
        from 
            cancel_base where source_id != 3 and platform_id != 7
        ) t 
   group by 
   supplier_num,
   source_id,
   source_name,
   platform_id,
   platform_name
   grouping sets((supplier_num), -- 0 0
                (supplier_num,source_id,source_name), -- 1 0
                (supplier_num,source_id,source_name,platform_id,platform_name)
                )
    ) 
    '''
    _sql = '''select 
a.supplier_num,                                                 --供应商编码
COALESCE(CAST(source_id AS int),0) AS source_id,
COALESCE(source_name,'全部') AS source_name,
COALESCE(CAST(platform_id AS int),0) AS platform_id,
COALESCE(platform_name,'全部') AS platform_name,
round(prod_sale_fixed_amt,4),                                   --支付码洋
round(a.prod_sale_amt,4),                                       --支付实洋
pay_cust_num,                                                   --支付用户数
pay_order_num,                                                  --支付订单数
pay_product_num,                                                --支付商品件数
cancel_order_num,                                               --取消订单数
round(a.prod_sale_amt/b.prod_sale_amt,4) prod_sale_amt_rate,    --支付实洋占比(每个渠道支付实洋占供应商总支付实洋的比例)
round(a.prod_sale_amt/pay_cust_num,4) pay_atv,                  --支付客单价（支付实洋/支付用户数）
round(cancel_order_num/pay_order_num,4) cancel_order_rate,
'{date_end}' as data_date
from union_table a
left join (
select 
supplier_num,
sum(prod_sale_amt) prod_sale_amt
from
pay_base
group by 
supplier_num
)b 
on a.supplier_num = b.supplier_num'''.format(date_end = date_end)

    sql = pay_sql+cancel_sql+t+_sql
    return sql

@logrecord(supplier_logger)
def do_job(date):

    _order_overview_table = "dm_report.dm_vrm_order_overview_{}"
    date_begin = date
    date_end = date

    columns = order_overview.keys()
    columns_list = ','.join(columns)
    _where = "supplier_num = '{supplier_num}' and source_id = {source_id} and platform_id = {platform_id} and data_date = '{data_date}'"
    _dev_fetch_sql = "select " + columns_list + " from {table}  where {where} "

    hive_db = PyHive()

    for date_type in ['d','wtd','mtd']:
        order_overview_table = _order_overview_table.format(date_type)

        if date_type == 'wtd':
            date_strp = datetime.strptime(date, '%Y-%m-%d')
            date_begin = datetime.strftime(date_strp-datetime.timedelta(date_strp.weekday()),'%Y-%m-%d')
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

                    diffvalue:dict = diff(test_item_hive, dev_item_hive)
                    print(where)
                    # if diffvalue != {}:
                    #     where = where+" "+date_type+"-Fail-"
                    #     supplier_logger.info(where)
                    #     supplier_logger.info(diffvalue)
                    #     supplier_logger.info('')
                    # else:
                    #     where = where+" "+date_type+"-Success-"
                    #     supplier_logger.info(where + "\n")
                    yield where, diffvalue
    hive_db.close_db()

    pass


if __name__ == '__main__':
    date = '2022-04-17'

    do_job(date)