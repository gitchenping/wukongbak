'''
阅读身份卡用户价值标签：

用户价值-累计购买图书数量-历史以来正向且图书订单购买商品件数总和
用户价值-当年购买图书数量-当年正向且图书订单购买的商品商品件数总和
'''
import queue,random
from threading import Thread
import logging.config
from utils.util import simplediff
from utils.db import connect_hive

table = "dm_report.usertags_reading_card_cust_buy_book_num"
usertags_reading_card_cust_buy_book_num = {
  'cust_id':'cust id',
  'buy_book_num_total' : '累计购买图书数量',
  'buy_book_num_ytd': '当年购买图书数量'
}

logging.config.fileConfig("logging.conf")
card_logger=logging.getLogger('view')

def get_hive_result(sql):
    """ 根据sql条件查询结果并返回"""
    cursor = connect_hive()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    return result


def get_test_hsql(date):
    '''
    指标sql
    :return:
    '''
    sql ='''WITH combine_custs_base AS(
        SELECT a.cust_id,
        coalesce(b.combine_cust_id, a.cust_id) AS main_cust_id
        FROM
        (SELECT *
         FROM 
         dm_report.usertags_base_active_custs
         WHERE data_date = '{date}') a
         LEFT JOIN
         (SELECT *
            FROM dm_report.usertags_reading_card_cust_map_base
         WHERE data_date = '{date}') b ON (a.cust_id = b.cust_id)),
         pay_without_cancel_orders AS(
         SELECT *
           FROM dm_report.usertags_reading_card_pay_orders_base
           WHERE data_date <= '{date}'
                -- AND data_date > add_months('{date}', -36) 
                AND bd_id IN (5, 12) 
                AND sale_type IN (1) 
                AND order_id not in (
                SELECT order_id
                FROM dm_report.usertags_reading_card_cancel_orders_base
                WHERE data_date <= '{date}'
                -- and data_date > add_months('{date}', -36)
                GROUP BY order_id 
            )
            ),
        combine_custs_pay_orders AS(
        SELECT a.main_cust_id,b.*
        FROM combine_custs_base a
        JOIN pay_without_cancel_orders b ON (a.cust_id = b.cust_id)),
        combine_custs_pay_orders_rank as(
        SELECT 
        t.*,
        ROW_NUMBER() OVER(PARTITION BY t.main_cust_id ORDER BY t.pay_date DESC ) as rank
        FROM combine_custs_pay_orders t
        )
       SELECT main_cust_id,
          --max(product_name) AS product_name,
          sum(prod_sale_qty) AS buy_book_num_total,
          sum(CASE WHEN year(pay_date) = year('{date}') THEN prod_sale_qty ELSE 0 END) AS buy_book_num_ytd
       FROM combine_custs_pay_orders
        --FROM combine_custs_pay_orders_rank
        -- where rank<=100
        GROUP BY main_cust_id'''

    return sql.format(date = date)

    pass


def w_job(q,date):
    '''
    向队列写入
    :param date:
    :return:
    '''
    columns = usertags_reading_card_cust_buy_book_num.keys()
    columns_list = ','.join(columns)
    dev_fetch_sql = "select "+ columns_list +" from "+table+" where cust_id = {cust_id} and data_date = '{data_date}'"

    sql = get_test_hsql(date)
    print(sql)
    test_hive_result = get_hive_result(sql)
    # test_hive_result = [(319188402,41,7),(697506404,10,1),(204778325,19,0)]
    test_hive_result_length = len(test_hive_result)

    if test_hive_result_length >0:
        sample_list = random.sample([i for i in range(test_hive_result_length)],min(1000,test_hive_result_length))
        for i in sample_list:
            test_item_hive = dict(zip(columns,test_hive_result[i]))
            dev_fetch_sql_format = dev_fetch_sql.format(cust_id = test_item_hive['cust_id'],data_date = date)

            dev_item_hive = get_hive_result(dev_fetch_sql_format)
            if len(dev_item_hive) >0:
                dev_item_hive = dict(zip(columns,dev_item_hive[0]))
            else:
                dev_item_hive = {} #dev table miss

            item_tuple = (test_item_hive,dev_item_hive)
            print(item_tuple)
            q.put(item_tuple)

            pass

def r_job(q,date):
    '''
    比较并写日志
    :param q:
    :param date:
    :return:
    '''
    while 1:
        try:
            item = q.get(block=True, timeout = 10)
            test_result = item[0]
            dev_result = item[1]

            diffvalue = simplediff(test_result,dev_result)
            message = 'cust_id = ' + str(test_result['cust_id']) + " and data_date = '" + date + "'"
            if diffvalue != {}:
                message += "-Fail-"
                card_logger.info(message)
                card_logger.info(diffvalue)
                card_logger.info('')
            else:
                message += "-Success-"
                card_logger.info(message+"\n")

        except queue.Empty:
            break


if __name__ == '__main__':

    date = '2022-01-24'
    q = queue.Queue()
    pw = Thread(target = w_job,args=(q,date))
    pr = Thread(target = r_job,args=(q,date))
    pw.start()
    pr.start()
    q.join()