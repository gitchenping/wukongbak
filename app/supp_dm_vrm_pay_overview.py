'''
实时作战室
小时段 只计算支付实洋
all 计算所有
'''

import os
from os import path
import logging
import time,json,math,random,pickle
from decimal import Decimal
from kafka import KafkaConsumer,TopicPartition

from utils.decorate import logrecord
from utils.util import diff
from utils.db import PyHive,PyCK,PyMongo
from .supp_dm_vrm_flow_pay_prod_hour import flow_weidu_dict,pay_weidu_dict,where

#logger
filepath=path.join(path.dirname(path.dirname(__file__)),"conf","logging.conf")
logging.config.fileConfig(filepath)
supplier_logger=logging.getLogger('jingying_overview')


#目标库
dev_info ={
    'table' : 'supp_dm_vrm_flow_pay_overview',
    'columns' :{
        'supplier_num': '0016151',
        'source_id': 0,
        'source_name': '全部',
        'platform_id': 0,
        'platform_name': '全部',
        'pv': 0,
        'uv': 0,
        'pay_cust_num': 8,
        'pay_order_num': 8,
        'pay_product_num': 8,
        'prod_sale_amt': 374.0,
        'data_date': '2022-06-06',
        'data_hour': '10'
    },
    'where':{'data_date':'','data_hour': '','supplier_num': '','source_id': 0,'platform_id':0},
    'src_conn':PyCK(),
    # 'src_conn':PyMysql(database= 'test'),
    'dst_conn':PyMongo(host ='XX',port = '8635',user ='rwuser',password= 'XX',
                   database='supp_db',collection='supp_dm_vrm_flow_pay_overview')
}



#流量平台
platform_key ={
    'date_str' :'',
    'hour_str':'' ,
    'data_server':'' ,
    'last_arri_supp_no':'',
    'uniq_id':'',
    'device_id':'',
    'from_platform':'',
    'bd_id_prod':''
}
#支付数据
pay_key = {
    'uuid':'',
    'dt':'',
    'hr':'',
    'data_type':'',
    'item_id':0,
    'order_id': 0,
    'order_status':0,
    'cust_id':0,
    'order_type':'',
    'permanent_id':'',
    'bargin_price':0.00,
    'order_quantity':0,
    'source':'',
    'platform':'',
    'last_arri_supp_no':'',
    'last_change_date':''

}


class Kafka_Supp:
    def __init__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers='XX:9092,1XX:9092,XX:9092,' \
                              'XX:9092,1XX:9092,XX:9092',  # kafka 地址
            auto_offset_reset = 'earliest',  # 消费模式
            enable_auto_commit = False,  # 自动提交消费数据的offset， False 不提交
            max_poll_records = 1,  # 每次消费做大的数据量，默认500
            group_id='group_supp',  # 消费组
            consumer_timeout_ms = 10000,  # 如果1秒内kafka中没有可供消费的数据，自动退出
            # value_deserializer=lambda m: json.loads(m.decode('ascii')),  # 消费json 格式化内容
            # key_deserializer=lambda k: pickle.loads(k),
        )
    #
    def consumer_seek(self, topic = None,partition_num = 0,timestamp_begin = '',date_str_end=''):
        """
        :param topic: 主题
        :param partition_num: 指定消费的分区
        :param offset_num: 该分区指定的消费位置
        :param offset_end_num: 该分区指定的消费结束的位置
        :return:
        """
        partition = TopicPartition(topic= topic, partition= partition_num)
        self.consumer.assign([partition])  #指定分区
        begin_offset = self.consumer.beginning_offsets([partition]).get(partition)
        end_offset = self.consumer.end_offsets([partition]).get(partition)
        offset_seek = begin_offset
        time_interval = 0.1*60*60*1000
        while begin_offset <= end_offset:
            mid_offset = (begin_offset + end_offset) // 2
            self.consumer.seek(partition, offset = mid_offset)
            a = self.consumer.position(partition)
            res = next(self.consumer)
            time_delta = res.timestamp - timestamp_begin
            if time_delta > 0 :
                end_offset = mid_offset - 1
            elif time_delta < -time_interval:
                begin_offset = mid_offset + 1
            else:
                while True:
                    res = next(self.consumer)
                    if res.timestamp >= timestamp_begin:
                        offset_seek = self.consumer.position(partition) - 1
                        break
                break
        self.consumer.seek(partition,offset_seek)
    #
    def get_partitions_for_topic(self,topic):
        partition = TopicPartition(topic=topic, partition=1)
        self.consumer.assign([partition])
        a = self.consumer.position(partition)
        return self.consumer.partitions_for_topic(topic)
    #
    def close_consumer(self):
        self.consumer.close()




def dump_file(filename,data):
    with open(the_local+'/source/'+filename+".pickle",'wb') as f:
        pickle.dump(data,f)

def load_file(filename):
    with open(the_local+'/source/'+filename+".pickle",'rb') as f:
        data = pickle.load(f)
    return data

def do_job_one(data_server,timeStamp_begin):
    '''

    :param data_server:
    :param timeStamp_begin:
    :return:
    '''

    consumer = Kafka_Supp()
    topic_list = ['mobile_client_expose','ddclick_expose',
                  #'mobile_wap_expose',
                  'miniprogram_expose','dwd_order_items_details_pay']


    for topic in topic_list[-1:]:
        data = []
        partitions_list = list(consumer.get_partitions_for_topic(topic)) # {0,1,2,3，4,5}
        if topic != 'dwd_order_items_details_pay':
            source_key = platform_key.keys()
            data_server_key = 'data_server'
        else:
            source_key = pay_key.keys()
            data_server_key = 'last_change_date'

        for partition in partitions_list:
            consumer.consumer_seek(topic,partition,timeStamp_begin)
            for ele in consumer.consumer:

                json_ele = json.loads(ele.value)
                if json_ele[data_server_key] <= data_server:
                    #将数据写入列表
                    temp = [json_ele[key] for key in source_key ]
                    if json_ele['last_arri_supp_no'] !='null':
                        partition_num = ele.partition
                        offset = ele.offset
                        data.append([partition_num,offset] + temp)
                else:
                    break
        dump_file(topic,data)

    pass



def do_job_two():
    mysql = PyMysql(database='test')
    topic_list = ['mobile_client_expose', 'ddclick_expose',
                  # 'mobile_wap_expose',
                  'miniprogram_expose', 'dwd_order_items_details_pay']
    #建数据表
    for topic in topic_list[0:1]:
        table_name = topic + "_source"
        sql = "drop table if exists " + table_name
        mysql.cursor.execute(sql)
        if topic.endswith('pay'):
            columns_key = pay_key.keys()
        else:
            columns_key = platform_key.keys()
        columns_str = ','.join([key + " varchar(50)" for key in columns_key])
        sql = "create table {table_name} (partiton int,offset bigint ,{columns_str})".format(table_name=table_name, columns_str=columns_str)
        mysql.cursor.execute(sql)
    #写数据库
    for topic in topic_list[0:]:
        data_list = load_file(topic)
        length = len(data_list)
        if topic.endswith('pay'):
            columns_len = len(pay_key)
        else:
            columns_len = len(platform_key)
        _value= "%s,"*columns_len

        sql = "insert into " + topic + "_source values(%s,%s,{value})".format(value =_value.strip(','))
        i=0
        while i<= length // 10:
            item = data_list[10*i:10*i+10]
            if item !=[]:
                a = mysql.cursor.executemany(sql,item)
                mysql.conn.commit()
            i += 1

    mysql.close_db()




@logrecord(supplier_logger)
def do_job_three(date_str,hour_str):
    # test_sql = sql.format(date_str = date_str,hour_str = hour_str)
    _test_sql = get_test_sql()

    if hour_str != 'all':
        hour_str_where = " and hour_str = '{}' ".format(hour_str)
    else:
        hour_str_where = ''
        hour_str = 'all'

    test_sql =  _test_sql.format(**flow_weidu_dict,**pay_weidu_dict,**where,date_str = date_str ,hour_str_where =hour_str_where, hour_str = hour_str)
    print(test_sql)
    columns_keys = dev_info['columns'].keys()
    _where = dev_info['where']


    src_hd = dev_info['src_conn']
    dst_hd = dev_info['dst_conn']
    test_result = src_hd.get_result_from_db(test_sql)
    test_result_length = len(test_result)

    if test_result_length > 0:
        sample_list = random.sample([i for i in range(test_result_length)], min(500, test_result_length))
        for i in sample_list:
            test_item = dict(zip(columns_keys, test_result[i]))
            mongo_where = {key:test_item[key] for key in _where.keys()}

            dev_item = dst_hd.coll.find_one(mongo_where)

            if hour_str !='all':
                prod_sale_amt = test_item['prod_sale_amt']
                if prod_sale_amt !=0:
                    test_item = {'prod_sale_amt':prod_sale_amt}
                else:
                    test_item = {}

            if dev_item is not None:
                dev_item.pop('_id')
                if hour_str != 'all':
                    dev_item = {'prod_sale_amt': dev_item['prod_sale_amt']}
                if dev_item.__contains__('prod_sale_amt'):
                    dev_item['prod_sale_amt'] = round(dev_item['prod_sale_amt'],2)
            else:
                print(test_item) #{}
                dev_item = {}  # dev table miss

            diffvalue = diff(test_item, dev_item, ['data_date'])
            print(where)
            yield str(mongo_where), diffvalue

    pass


if __name__ == '__main__':

    date_str_begin = '2022-06-06 09:50:00'
    date_str_end = '2022-06-06 11:02:00'
    data_server = date_str_end
    timeArray = time.strptime(date_str_begin, "%Y-%m-%d %H:%M:%S")
    timeStamp_begin = int(time.mktime(timeArray)) * 1000
    timeArray = time.strptime(date_str_end, "%Y-%m-%d %H:%M:%S")
    timeStamp_end = int(time.mktime(timeArray)) * 1000

    date_str = '2022-06-14'
    # do_job_one(data_server,timeStamp_begin)
    # do_job_two()
    for hour_str in ['10','all'][0:1]:
        do_job_three(date_str,hour_str)
