from ._sql import gen_sqldata
from utils.util import connect_hive,connect_clickhouse
# 连接ck
conn_ck = connect_clickhouse(host='10.12.6.116', database='ioc_mdata')
# 连接hive
hive_cursor = connect_hive()


def collect_offline(date):
    '''离线收藏 clickhouse sql'''
    data_date=date

    offline_sql='''
            SELECT
                t.cust_id,
                t.product_id,
                t.shop_id,
                t.brand,
                t.category_path,
                count(*) AS pv,
                concat(t.data_date,' 00:00:00') AS creation_date,
                t.supply_id,
                t.data_date
            FROM
                (
                SELECT
                    t1.cust_id,
                    t1.product_id,
                    t1.shop_id,
                    t2.brand,
                    t2.category_path,
                    t3.last_supplier_id AS supply_id,
                    t1.data_date
                FROM
                    (
                    SELECT
                        cust_id,
                        product_id,
                        shop_id,
                        data_date
                    FROM
                        ddclick_umt.product_wish_info
                    WHERE
                        data_date = '{}') t1
                LEFT JOIN (
                    SELECT
                        product_id,
                        brand,
                        category_path
                    FROM
                        productdb.prod_basic) t2 ON
                    (t1.product_id = t2.product_id)
                LEFT JOIN (
                    SELECT
                        item_id,
                        last_supplier_id
                    FROM
                        dw_ods.item_book
                    WHERE
                        trans_date = '{}'
                        AND item_id IS NOT NULL
                        AND last_supplier_id IS NOT NULL
                    GROUP BY
                        item_id,
                        last_supplier_id) t3 ON
                    (t1.product_id = t3.item_id)) t
            GROUP BY
                t.cust_id,
                t.product_id,
                t.shop_id,
                t.brand,
                t.category_path,
                t.supply_id,
                t.data_date
                '''.format(data_date,data_date)
    return offline_sql

def get_collect_offline_hive_data(date):

    offline_sql=get_collect_offline_hive_data(date)

    #数据量较大，使用生成器
    # hive_cursor.execute(offline_sql)
    # hive_data = hive_cursor.fetchall()
    hive_data=gen_sqldata(offline_sql,hive_cursor,10)
    return hive_data


