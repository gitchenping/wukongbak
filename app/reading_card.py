'''
阅读身份卡-
用户价值-累计购买图书数量-历史以来正向且图书订单购买商品件数总和
用户价值-当年购买图书数量-当年正向且图书订单购买的商品商品件数总和
'''

import random
import queue
from threading import Thread
from  utils.db import PyHive
from utils.util import diff
from utils.decorate import logrecord
from utils.log import get_logger

#目标表结构
table = "dm_report.usertags_reading_card_cust_info_all"
usertags_reading_card_cust_info = {
  'cust_id' : 'cust id',
    #'用户历史购买正向图书中的商品ID，按照购买时间倒序取前200个，用逗号分割'
  'product_ids' : '近期购买TOP200品',
    #'累计购买图书数量-历史以来正向且图书订单购买商品件数总和'
  'total_pay_book_num' : '累计购买图书数量',
    #累计图书阅读字数(单位：万字)-历史以来正向且图书订单中商品字数*商品件数和；如果商品没有字数默认按照一本童书5万字、非童书15万字计算
  'total_read_word_num' : '累计图书阅读字数',
    #用户累积购买的图书数量(去重)
  'total_pay_path2_num' : '累计购买图书品种数',
    #用户购买最多的二级品类id
  'pay_1st_path2' : 'top1品类偏好',
  'pay_1st_path2_name' : '用户购买最多的二级品类名称',
  'pay_1st_path2_num' : '用户购买最多的二级类商品数量(去重)',
    #用户购买第二多二级品类id
  'pay_2nd_path2' : 'top2品类偏好',
  'pay_2nd_path2_name' : '用户购买第二多二级品类名称',
  'pay_2nd_path2_num' : '用户购买第二多的二级类商品数量(去重)',
  #用户购买第三多二级品类id
  'pay_3rd_path2': 'top3品类偏好',
  'pay_3rd_path2_name': '用户购买第三多二级品类名称',
  'pay_3rd_path2_num': '用户购买第三多的二级类商品数量(去重)'
}

#logger
reporter = get_logger(filename = 'user_value.txt')


def get_test_hsql(date):
    '''
    指标sql
    :return:
    '''
    sql ='''WITH all_cust_ids AS( --所有cust_id
    SELECT *
    FROM dm_report.usertags_reading_card_cust_id_all
    WHERE data_date = '{data_date}'   limit 100),
    base_create_orders AS( --收订
    SELECT t1.*,
          t2.main_cust_id
    FROM
     (SELECT *
      FROM dm_report.usertags_reading_card_create_orders_base
      WHERE bd_id IN (5, 12)
        AND sale_type IN (1)
        AND data_date <= '{data_date}') t1
      INNER JOIN all_cust_ids t2 ON (t1.cust_id = t2.cust_id)),
    base_cancel_table AS(
        select 
            t.*
        from(
        SELECT t2.item_id, t1.cust_id, t1.parent_id, t1.order_id,t2.product_id,t3.bd_id,
        t2.order_quantity AS prod_sale_qty,t1.order_creation_date AS cancel_date,t1.data_date AS data_date
        FROM ods.orders_all t1
        LEFT JOIN ods.order_items_all t2 ON (t1.order_id = t2.order_id)
        LEFT JOIN dim.dim_prod_basic_whole t3 ON (t2.product_id = t3.product_id)
        WHERE t1.data_date >='2000-01-01' AND t1.data_date <'2019-01-01'
        AND t2.data_date >='2000-01-01' AND t2.data_date  <'2019-01-01'
        -- and t1.cust_id in (241018600,757999852,639194947,688224046,11622076,757490726,105591614,634610590,751880797,630884775)
        AND t1.order_status = -100
        AND (t1.order_type IS NULL  OR t1.order_type != 21)
        AND t3.bd_id IN (5, 12)
        and (CAST(t1.order_type AS int) IS NULL OR CAST(t1.order_type AS int) NOT IN (3, 50, 51, 166, 168)) 
        --product_type=101影分身品,product_type=9虚拟捆绑品
        AND (CAST(t2.product_type AS int) IS NULL OR CAST(t2.product_type AS int) NOT IN (9, 101)) -- 剔除测试店铺
        AND (CAST(t1.shop_id AS int) IS NULL OR CAST(t1.shop_id AS int) NOT IN (9308, 9309, 9307, 9302, 9276, 7580, 4733, 489, 170, 4428))
        )t
        LEFT JOIN ods.order_test_user test ON (test.cust_id = t.cust_id)
        WHERE test.cust_id IS NULL
        union all 
        SELECT t1.item_id,t1.cust_id,t1.parent_id,t1.order_id,t1.product_id,t1.bd_id,
        t1.prod_sale_qty, t1.cancel_date,t1.data_date AS data_date
        FROM dm_dws.dm_order_cancel_detail t1
        WHERE t1.data_date >= '2019-01-01' AND t1.data_date < date_sub('2022-02-25',-1)
        AND t1.`source` = 1 AND t1.bd_id IN (5, 12)
        -- and t1.cust_id in (241018600,757999852,639194947,688224046,11622076,757490726,105591614,634610590,751880797,630884775)
    ),
    base_cancel_orderids AS(  --取消
    SELECT t1.order_id
    FROM  base_cancel_table t1
    INNER JOIN all_cust_ids t2 ON (t1.cust_id = t2.cust_id)
    WHERE t1.data_date <= '{data_date}'
    GROUP BY t1.order_id),
    base_back_orders AS(   --销退
    SELECT t1.*,
          t2.main_cust_id
   FROM dm_report.usertags_reading_card_back_orders_base t1
   INNER JOIN all_cust_ids t2 ON (t1.cust_id = t2.cust_id)
   WHERE t1.data_date <= '{data_date}'),
   create_without_cancel_and_back_orders AS(
   select a.*
   from (
   SELECT t1.cust_id,
          t1.parent_id,
          t1.order_id,
          t1.product_id,
          t1.number_of_words,
          t1.bd_id,
          t1.category_path2, --'二级品类id'
          t1.path2_name,    --'二级品类名称'
          t1.order_creation_date,
          t1.prod_sale_qty + coalesce(t2.prod_sale_qty, 0) AS prod_sale_qty, --'此分类购买商品数量'
          t1.main_cust_id
   FROM base_create_orders t1
   LEFT JOIN base_back_orders t2 ON (t1.item_id = t2.item_id)
   LEFT JOIN base_cancel_orderids t3 ON (t1.order_id = t3.order_id)
   WHERE t3.order_id IS NULL and regexp_extract(t1.category_path2,'([0-9]+)',1) = '01') a
   where a.prod_sale_qty >0),
   custs_info_like_category2 AS(  --group by cust_id & category_path2 3*2个指标
   SELECT t.*,
          ROW_NUMBER() OVER(PARTITION BY t.main_cust_id ORDER BY t.prod_sale_qty DESC) AS rank,
          '{data_date}' as data_date
   FROM
     (SELECT main_cust_id,
             category_path2,
             max(path2_name) AS path2_name,
             sum(prod_sale_qty) AS prod_sale_qty,
             count(DISTINCT product_id) AS prod_distinct_num
      FROM create_without_cancel_and_back_orders
      GROUP BY main_cust_id,category_path2) t),
   custs_info_like_product AS(  --group by  cust_id & product_id 1个指标
   SELECT t.*, 
        ROW_NUMBER() OVER(PARTITION BY t.main_cust_id ORDER BY t.order_creation_date DESC) AS rank,
        '{data_date}' as data_date
   FROM (
    SELECT main_cust_id, product_id, max(order_creation_date) AS order_creation_date 
    FROM create_without_cancel_and_back_orders 
    GROUP BY main_cust_id, product_id) t
    ),
    custs_info_buy_book_num AS(   --group by cust_id 3个指标
    SELECT main_cust_id,
    sum(prod_sale_qty) AS buy_book_num_total,  --'累计购买图书数量'
    -- sum(CASE WHEN year(order_creation_date) = year('{data_date}') THEN prod_sale_qty ELSE 0 END) AS buy_book_num_ytd,
    count(DISTINCT product_id) AS buy_book_distinct_num, --'累计购买图书品种数'
    sum(prod_sale_qty * if(number_of_words IS NOT NULL AND number_of_words > 0,cast(number_of_words/10000 as decimal(38,4)),
     CASE WHEN bd_id = 12 THEN cast(5 as decimal(38,4)) WHEN bd_id=5 THEN cast(15 as decimal(38,4)) ELSE 0 END)
    ) AS number_of_words_total                --累计图书阅读字数
    FROM create_without_cancel_and_back_orders
    GROUP BY main_cust_id)
    select 
        t1.main_cust_id as cust_id,
        t2.product_ids as product_ids,
        coalesce(t3.buy_book_num_total,0) AS total_pay_book_num,
        cast(coalesce(t3.number_of_words_total,0) AS bigint) AS total_read_word_num,
        coalesce(t3.buy_book_distinct_num, 0) AS total_pay_path2_num,
        t4.category_path2 AS pay_1st_path2,
        t4.path2_name AS pay_1st_path2_name,
        coalesce(t4.prod_distinct_num,0) AS pay_1st_path2_num,
        t5.category_path2 AS pay_2st_path2,
        t5.path2_name AS pay_2st_path2_name,
        coalesce(t5.prod_distinct_num,0) AS pay_2st_path2_num,
        t6.category_path2 AS pay_3rd_path2,
        t6.path2_name AS pay_3rd_path2_name,
        coalesce(t6.prod_distinct_num,0) AS pay_3rd_path2_num
    from (select main_cust_id 
            from all_cust_ids group by main_cust_id) t1 
    left join (
        select main_cust_id,
            regexp_replace(concat_ws(',', sort_array(collect_list(concat_ws(':',lpad(cast(rank AS string),3,'0'),cast(product_id AS string))))), '\\\\d+\\\\:','') AS product_ids
        from custs_info_like_product 
        where data_date = '{data_date}' and rank <=200 group by main_cust_id) t2 
    on t1.main_cust_id = t2.main_cust_id
    left join (
        select * 
        from custs_info_buy_book_num 
    ) t3
    on t1.main_cust_id = t3.main_cust_id
    LEFT JOIN (
    SELECT * 
    FROM custs_info_like_category2  
    WHERE data_date='{data_date}' AND rank = 1) t4 
    ON (t1.main_cust_id = t4.main_cust_id)
    LEFT JOIN (
    SELECT * 
    FROM custs_info_like_category2  
    WHERE data_date='{data_date}' AND rank = 2) t5 
    ON (t1.main_cust_id = t5.main_cust_id)
    LEFT JOIN (
    SELECT * 
    FROM custs_info_like_category2   
    WHERE data_date='{data_date}' AND rank = 3) t6 
    ON (t1.main_cust_id = t6.main_cust_id)
    where t3.buy_book_num_total >0
    '''
    return sql.format(data_date = date)

    pass

def w_job(q,date):
    '''
    向队列写入
    :param date:
    :return:
    '''
    columns = usertags_reading_card_cust_info.keys()
    columns_list = ','.join(columns)
    dev_fetch_sql = "select "+ columns_list +" from "+table+" where cust_id = {cust_id} and data_date = '{data_date}'"

    sql = get_test_hsql(date)
    print(sql)

    hive_handler = PyHive()
    test_hive_result = hive_handler.get_result_from_db(sql)
    # test_hive_result = [(337050908,'27886199',1,20,1,'01.21.00.00.00.00','成功/励志',1,None,None,0),
    # (264090423,'25182503,24194942',2,30,2,'01.31.00.00.00.00','心理学',1,'01.43.00.00.00.00','中小学教辅',1)]
    test_hive_result_length = len(test_hive_result)

    if test_hive_result_length >0:
        sample_list = random.sample([i for i in range(test_hive_result_length)],min(50,test_hive_result_length))
        for i in sample_list:
            test_item_hive = dict(zip(columns,test_hive_result[i]))
            dev_fetch_sql_format = dev_fetch_sql.format(cust_id = test_item_hive['cust_id'],data_date = date)

            dev_item_hive = hive_handler.get_result_from_db(dev_fetch_sql_format)
            if len(dev_item_hive) >0:
                dev_item_hive = dict(zip(columns,dev_item_hive[0]))
            else:
                dev_item_hive = {} #dev table miss

            item_tuple = (test_item_hive,dev_item_hive)
            print(item_tuple)
            q.put(item_tuple)

            pass
        #放入一个结束标志
        q.put((0,0))
        print('write done')


    pass


@logrecord(reporter)
def r_job(q,date):
    '''
    比较并写日志
    :param q:
    :param date:
    :return:
    '''
    while 1:

        item = q.get(block=True)
        if item == (0,0):
            print('read complete')
            break
        else:
            test_result = item[0]
            dev_result = item[1]

            diffvalue = diff(test_result,dev_result)
            where_msg = 'cust_id = ' + str(test_result['cust_id']) + " and data_date = '" + date + "'"
            yield where_msg,diffvalue
    pass



if __name__ == '__main__':

    date = '2022-02-24'
    q = queue.Queue()
    pw = Thread(target = w_job,args=(q,date))
    pr = Thread(target = r_job,args=(q,date))
    pw.start()
    pr.start()
    q.join()