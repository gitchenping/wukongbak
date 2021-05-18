import os,sys
from utils import util
from utils import log
from db.map.crm import product_dict,mayang_yunying_dict,warehouse_dict
from db.dao.crm import get_crm_sql_data,get_crm_product_month_year_top_sql
from utils.util import connect_mysql_from_jump_server

#mysql 信息字典
sql_db_info={
    'host':'myBdataSupplierDB.db',
    'port':3306,
    'user':'BdataSupplie_rw',
    'password':'my@#6rnY9nGQRW',
    'database':"BdataSupplierDB"
}

#hive cursor
# hive_cursor= util.connect_hive()

def hive_mysql_diff(logger,hive_sql,mysql_table,tableinfo_key,date,month):
    '''

    :param sql: hive sql
    :return:
    '''
    # hive_cursor.execute(hive_sql)
    # hive_data = hive_cursor.fetchmany(size=10000)

    #通过文件读取hive_data
    hive_data=[]
    if mysql_table.startswith('crm_supply_mayang_yunying_month'):
        value_l=True
    else:
        value_l=False

    for ele in hive_sql:
        a=[column.strip(' ') for column in ele.strip('\n').split('|') if column != '']
        if value_l:
            a = a[0:-8] + [round(float(column),2) if column != 'NULL' else None for column in a[-8:]]
        else:
            a=a[0:-4]+[float(column) for column in a[-4:]]

        hive_data.append(a)

    if os.name == 'posix':
        mysql_cursor = util.connect_mysql(**sql_db_info)
    else:
        server, mysql_cursor = connect_mysql_from_jump_server(*sql_db_info.values())
    # 分批次查询
    i = 0
    step = 10
    cover = int(step / 2)
    while i < 1000:
        each_patch_data = {}
        datalist = hive_data[i:i + cover]
        if datalist == []:
            break
        for each in datalist:
            data = dict(zip(tableinfo_key, each[1:]))
            supplier_num = data.pop('supplier_num')

            key=supplier_num
            if data.__contains__('warehouse_name'):
                warehouse_name = data.pop('warehouse_name')
                data['warehouse_id']=int(data['warehouse_id'])
                key = key + '_' + warehouse_name
            if data.__contains__('product_id'):
                product_id = data.pop('product_id')
                key=key + '_' + product_id

            each_patch_data[key] = data
        i += step

        mysql_data = get_crm_sql_data(each_patch_data.keys(), tableinfo_key, mysql_cursor, mysql_table, date, month)
        diffkey = util.diff_dict(each_patch_data, mysql_data)
        if diffkey != {}:
            # logger.info('筛选条件: ' + str(filter) + "-*-Fail-*-")
            logger.info("diff info: " + str(diffkey))
            logger.info('')

    pass

#品排行 月、年
def product_rank_month_year(date,month=True):
    '''top品'''
    if month:
        logger = log.set_logger('product_rank_month.txt')
        mysql_table = "crm_supply_product_month_top"
    else:
        logger = log.set_logger('product_rank_year.txt')
        mysql_table = "crm_supply_product_year_top"


    hive_sql=get_crm_product_month_year_top_sql(date,month)
    #每个分组取top N
    # N=3
    # hive_sql="select * from ("+hive_sql+") t where rank<="+str(N)

    #hive_file ,通过文件
    if month:
        file='product_month.txt'
    else:
        file = 'product_rank_year.txt'
    with open('./loadfile/'+file,'r',encoding='UTF-8') as hive_sql:
        hive_mysql_diff(logger,hive_sql,mysql_table,product_dict.keys(),date,month)

def supply_warehose_rank(date):
    '''断货'''
    mysql_table='crm_supply_warehouse_month_top'
    logger = log.set_logger('warehouse_month_top.txt')

    # hive_sql=_sql.get_crm_warehouse_month_top_sql(date)
    with open('./loadfile/warehouse_month.txt', 'r',encoding='UTF-8') as hive_sql:
        hive_mysql_diff(logger, hive_sql, mysql_table,warehouse_dict.keys(), date, True)

    pass

def supply_mayang_yunying(date):
    mysql_table = 'crm_supply_mayang_yunying_month'
    logger = log.set_logger('mayang_yunying.txt')

    # hive_sql = _sql.get_crm_mayang_yunying_sql(date)
    with open('./loadfile/mayang_yunying_month.txt', 'r', encoding='UTF-8') as hive_sql:
        hive_mysql_diff(logger, hive_sql, mysql_table, mayang_yunying_dict.keys(), date, True)
    pass


def crm_test():
    data_date="2020-03-01"

    #月
    # product_rank_month_year(data_date)
    #年
    # product_rank_month_year(data_date,False)
    #断货
    supply_warehose_rank(data_date)
    #运营
    # supply_mayang_yunying(data_date)