import os
import sys
from os import path
import logging.config
import json,math,random,datetime
from utils.util import simplediff
from clickhouse_driver import Client,connect
from utils import db
from utils.load import readini
import pymysql
from utils.load import readconfini




def sqldecorate(obj):
    father_path = os.path.dirname(os.path.dirname(__file__))

    filepath = os.path.join(father_path,"config","testenv.ini")
    cf = readini.readini(filepath)


    return obj
def loadenv(**kwargs):
    env = readconfini('./conf/')

    def wrap_o(func):
        def wrap_in(self,**args):       #如果函数带参数，使用实参

            #host
            if args.__contains__('host') and args['host'] is not None:
                host=args['host']
            else:
                host = env.get(kwargs['db'], 'db_host')

            # port
            if args.__contains__('port') and args['port'] is not None:
                port = args['port']
            else:
                port = int(env.get(kwargs['db'], 'db_port'))

            #user
            if args.__contains__('user') and args['user'] is not None:
                user = args['user']
            else:
                user = env.get(kwargs['db'], 'db_username')

            #password
            if args.__contains__('password') and args['password'] is not None:
                password = args['password']
            else:
                password = env.get(kwargs['db'], 'db_password')

            #database
            if args.__contains__('database') and args['database'] is not None:
                database=args['database']
            else:
                database = env.get(kwargs['db'], 'db_name')

            #collection
            if args.__contains__('collection') and args['collection'] is not None:
                collection=args['collection']
            else:
                if env.has_option(kwargs['db'],'db_collection'):
                    collection = env.get(kwargs['db'],'db_collection')
                else:
                    collection = None
            func(self,host,port,user,password,database,collection)
        return wrap_in
    return wrap_o
    pass

class PyDB():

    def __init__(self):
        self.conn = None
        self.cusor = None
        pass

    def close_db(self):
        """关闭数据库"""
        if self.cursor is not None:
            self.cursor.close()
        if self.conn is not None:
            self.conn.close()

    def get_result_from_db(self,sql):

        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result




def load(func):

    def wrap(self,*args,**kw):
        host = '10.7.30.148'
        port = 9000
        user = 'membersbi'
        password = 'dangdangbi'
        database = 'ioc'
        func(self,host, port, user, password, database)
    return wrap


class PyCK(PyDB):

    @loadenv(db='db_ck')
    def __init__(self,host=None, port=None, user=None, password=None, database=None, collection=None):
        self.conn = Client(host=host, port=port, user=user, password=password, database=database)
        pass

    def get_result_from_db(self, sql):
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result
        pass


class PyMysql(PyDB):
    @loadenv(db='db_mysql')
    def __init__(self, host=None, port=None, user=None, password=None, database=None, collection=None):
        self.conn = pymysql.connect(host=host, port=port, user=user, password=password, db=database)
        self.cursor = self.conn.cursor()



def test_ck():

    ck_sql =" select * from dm_report.dm_reco_kpi_product  where product_id='22770934' " \
         "and date_str='2022-04-20' and platform='3' and shop_type=1"

    mysql_sql ="select * from project_info;"

    # ck_db = PyCK()
    # result = ck_db.get_result_from_db(ck_sql)


    mysql_db = PyMysql(host = '10.255.255.31',port=3307,user ='root',password ='123456',database='project')

    for i in range(1,4):
        sql ="select * from project_info where id = {};".format(i)
        result = mysql_db.get_result_from_db(sql)
        yield result

    mysql_db.close_db()




a= test_ck()
for ele in a:
    print(ele)