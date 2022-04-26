'''
用户生命周期:

--导入期：未下单的用户
-- 成长期：仅下一单用户
-- 成熟期：下多单且最后下单距离今天在90天内
-- 休眠期：下多单且最后下单距离今天在90-180天内
-- 流失期：下多单且最后下单距离今天在180天之外
'''
from utils.log import set_logger
from utils.db import PyHive

#logger
reporter = set_logger(filename = 'user_lifetime.txt')

def do_job():
    data_date = '2021-08-12'

    hive_db = PyHive()
    dev_table ="dm_report.usertags_user_life_time"
    check_column = ['cust_id','life_time_flag']
    column=','.join(check_column)

    ckeck_sql="select "+column+" from "+dev_table+" where cust_id in {} and data_date='{}'"

    test_sql = '''
        WITH custs_info AS(
        SELECT cust_id,
          count(DISTINCT parent_id) AS parent_nums,
          max(pay_date) AS last_parent_pay_time,
          datediff('{data_date}', substr(max(pay_date), 1, 10)) AS last_parent_time_range,
          '{data_date}' as data_date
        FROM dm_dws.dm_order_pay_detail
        WHERE data_date > add_months('{data_date}',-24)
            AND data_date <= '{data_date}'
            AND sale_type IN (1)
        GROUP BY cust_id)
        SELECT t1.cust_id,
          CASE
              WHEN t2.cust_id IS NULL THEN '导入期'
              WHEN t2.cust_id IS NOT NULL AND t2.parent_nums = 1 THEN '成长期'
              WHEN t2.cust_id IS NOT NULL
                   AND t2.parent_nums > 1
                   AND t2.last_parent_time_range <= 90 THEN '成熟期'
              WHEN t2.cust_id IS NOT NULL
                   AND t2.parent_nums > 1
                   AND t2.last_parent_time_range > 90
                   AND t2.last_parent_time_range <= 180 THEN '休眠期'
              ELSE '流失期'
          END AS life_time_flag
        FROM (SELECT cust_id FROM dm_report.usertags_base_active_custs WHERE data_date = '{data_date}') t1
        LEFT JOIN (SELECT * FROM custs_info WHERE data_date = '{data_date}') t2 ON t1.cust_id = t2.cust_id
        limit 100000
    '''
    test_sql_format = test_sql.format(data_date = data_date)

    print("测试sql:"+test_sql_format)

    hive_result = hive_db.get_result_from_db(test_sql_format)

    step = 500
    for i in range(0, len(hive_result), step):

        #每次取10个
        hiveresult=hive_result[i:i+10]
        print(hiveresult)

        hive_test = {}
        custid_list=[]
        for ele in hiveresult:
            custid_list.append(str(ele[0]))
            hive_test[ele[0]] = ele[1]

        custids="("+','.join(custid_list)+")"
        sql=ckeck_sql.format(custids,data_date)
        devresult = hive_db.get_result_from_db(sql)

        hive_dev={}
        for ele in devresult:
            key = ele[0]
            hive_dev[key]=ele[1]


        diff_key_value={}
        for key in hive_test.keys():

            try:
                data1_value = hive_test[key]
                data2_value = hive_dev[key]
            except Exception as e:
                print(str(key)+" "+e.__repr__())
                continue
            if data1_value != data2_value:
                diff_key_value[key] = {'test':data1_value, 'dev':data2_value}
                reporter.info(diff_key_value)


if __name__ == '__main__':
    do_job()