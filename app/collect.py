from util.util import connect_mongodb,connect_mysql,connect_hive,connect_clickhouse
import logging.config

logging.config.fileConfig("logging.conf")
collectlogger=logging.getLogger('collect')

def collect_realtime():
    collection = connect_mongodb(database='interface', collection='item_book')
    #连接mysql
    mysqlcursor=connect_mysql()
    #ck
    conn_ck=connect_clickhouse(host='10.12.6.116',database='default')

    fail=0
    success=0
    for table_lable in range(30):
        lable=str(table_lable)
        if table_lable<10:
            lable='0'+lable
        sql = "select customer_id,product_id from Favorite_6.favorite_"+lable+" where creation_date<'2020-12-24' and creation_date>'2020-12-23' "
        mysqlcursor.execute(sql)
        ret = mysqlcursor.fetchall()

        collectlogger.info("Favorite_6.favorite_" + lable)
        for customer_product in ret:
            custid=customer_product[0]
            productid=customer_product[1]

            # 从mongo中读取supplier_id
            mongo_supplyid=''
            a = collection.find({'product_id': productid})    #[{'_id': ObjectId('5fcde9b931d7e1f3fa1155d3'), 'product_id': 28663348, 'supply_id': 'DDKW7770972'}]
            list_a=list(a)
            if len(list_a)>0:
                mongo_supplyid=list_a[0]['supply_id']

            #ck
            ck_sql="select supply_id from default.collect_page_detail_local_v5_all  where cust_id ={} and product_id={}".format(custid,productid)
            b=conn_ck.execute(ck_sql)

            ck_supplyid=''
            if len(b)>0:
                ck_supplyid=b[0][0]
            # mongo和ck中的供应商比对

            if mongo_supplyid !=ck_supplyid:

                fail+=1
                collectlogger.info(str({productid:{'mongo':mongo_supplyid,'ck':ck_supplyid}}))
            else:
                success+=1
    print('success:'+str(success)+" fail:"+str(fail))


def collect_offline():
    data_date='2020-12-21'

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
    # 连接ck
    conn_ck = connect_clickhouse(host='10.12.6.116', database='ioc_mdata')
    #连接hive
    hivecursor=connect_hive()
    hivecursor.execute(offline_sql)
    hive_data = hivecursor.fetchall()

    fail=0
    success=0
    for hivedata in hive_data:
        cust_id=hivedata[0]
        product_id=hivedata[1]
        creation_date=hivedata[6]

        sql = "select toString(shop_id) as _shop_id,supply_id from ioc_mdata.collect_page_detail_all  where creation_date='{}' and cust_id={} and product_id={}" \
              "".format(creation_date,cust_id,product_id)
        conn_ck.execute(sql)
        ret = conn_ck.fetchall()

        diff_result=[]
        if ret!=[]:
            ret_data=ret[0]
            if ret_data[0] != hivedata[2]:
                diff_result.append({'shop_id': {'hive': hivedata[2], 'ck':ret_data[0] }})
            if ret_data[1] != hivedata[-2]:
                diff_result.append({'supply_id': {'hive': hivedata[-2], 'ck':ret_data[1] }})

        else:
                diff_result.append('ck不存在')

        if len(diff_result) > 0:
            # 写日志
            collectlogger.info(
                str('cust_id:' + str(cust_id) + " " + "product_id:" + str(product_id)+ " " + "creation_date:" + creation_date+ str(diff_result)))
            fail+=1
        else:
            success+=1
    print("success: "+ str(success)+", fail:"+str(fail))



