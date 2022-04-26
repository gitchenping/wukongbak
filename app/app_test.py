'''
测试脚本，适用本目录下脚本调试
'''

from utils.log import set_logger
from utils.db import PyHive,PyMysql
from utils.decorate import loaddbenv,logrecord
from utils.util import diff

#logger
reporter = set_logger()

#mysql
mysql_db = PyMysql(host = '10.255.255.31',port = 3307,user= 'root',password='123456',database= 'project')

@logrecord(reporter)
def do_job():
    data1 = {'a': 100, 'b': 20}
    data2 = {'a': 100, 'b': 22}

    sql = "select 1+99,2*15;"
    data_list = mysql_db.get_result_from_db(sql)

    for ele in data_list:
        test_item = dict(zip(['a','b'],ele))
        where = str(ele)

        diff_value = diff(test_item,data1)
        yield where,diff_value

    pass

if __name__ == '__main__':
    do_job()