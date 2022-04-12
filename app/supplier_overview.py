'''
供应商数据中心:交易分析-交易概况
'''

import os
import sys
import json,math,random
from datetime import datetime,timedelta
from db.dao.supplier_oveview import get_test_sql
import logging.config
from utils.util import simplediff

logging.config.fileConfig("logging.conf")
supplier_logger=logging.getLogger('view')

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

def do_job(date):

    _order_overview_table = "dm_report.dm_vrm_order_overview_{}"
    date_begin = date
    date_end = date

    columns = order_overview.keys()
    columns_list = ','.join(columns)
    _where = "supplier_num = '{supplier_num}' and source_id = {source_id} and platform_id = {platform_id} and data_date = '{data_date}'"
    _dev_fetch_sql = "select " + columns_list + " from {table}  where {where} "


    for date_type in ['d','wtd','mtd']:
        order_overview_table = _order_overview_table.format(date_type)

        if date_type == 'wtd':
            date_strp = datetime.strptime(date, '%Y-%m-%d')
            date_begin = datetime.strftime(date_strp-timedelta(date_strp.weekday()),'%Y-%m-%d')
        elif date_type == 'mtd':
            date_begin = date[0:-2]+"01"

        test_sql = get_test_sql(date_begin,date_end)
        print(test_sql)
        try :
            test_hive_result = get_hive_result(test_sql)
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

                    dev_item_hive = get_hive_result(dev_fetch_sql)
                    if len(dev_item_hive) > 0:
                        dev_item_hive = dict(zip(columns, dev_item_hive[0]))
                    else:
                        dev_item_hive = {}  # dev table miss

                    diffvalue = simplediff(test_item_hive, dev_item_hive)
                    print(where)
                    if diffvalue != {}:
                        msg = where+" "+date_type+"-Fail-"
                        supplier_logger.info(msg)
                        supplier_logger.info(diffvalue)
                        supplier_logger.info('')
                    else:
                        msg = where+" "+date_type+"-Success-"
                        supplier_logger.info(msg + "\n")

    pass

if __name__ == '__main__':
    date = '2022-04-10'

    do_job(date)