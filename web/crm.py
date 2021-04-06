'''
mysql_table={
    'crm_supply_cat_month':'月度品类销售分布',
    'crm_supply_zhengti_xiaoshou':'历史销售&回款',
    'crm_supply_mayang_yunying_month':'月毛利&存货',
    'crm_supply_caigou_month':'上月采购码洋到货率',
    'crm_supply_huigao_month':'上月回告码洋',

    'crm_supply_daohuoshichang_month':'月平均到货时长',
    'crm_supply_product_month_top':'月销售TOP40',
    'crm_supply_product_year_top':'年销售TOP40',
    'crm_supply_warehouse_month_top':'天津仓售断TOP全品'
}
'''
import os,sys
from utils import util,_sql
from utils import log
from .sql import sqldata,crm_sql_data
from utils.util import connect_mysql_from_jump_server

#mysql 信息字典
sql_db_info={
    'host':'myBdataSupplierDB.db',
    'port':3306,
    'user':'BdataSupplie_rw',
    'password':'my@#6rnY9nGQRW',
    'database':"BdataSupplierDB"
}

#品 字段信息
product_dict={
    'supplier_num':'供应商编码',
    'supplier_name' :'供应商名称',
    'isbn': 'isbn',
    'product_id':'商品id',
    'product_name':'商品名称',
    'category_path2' :'二级类路径',
    'path2_name': '二级类名称',
    'original_price':'定价',
    'prod_sale_qty': '销售数量',
    'prod_sale_fixed_amt':'销售码洋',
    'num' : '排行'
}

warehouse_dict={
    'supplier_num': '供应商编码',
    'supplier_name': '供应商名称',
    'warehouse_id':'仓店id',
    'warehouse_name':'仓店名称',
    'isbn': 'isbn',
    'product_id': '商品id',
    'product_name': '商品名称',
    'category_path2': '二级类路径',
    'path2_name': '二级类名称',
    'original_price': '定价',
    'prod_sale_qty': '销售数量',
    'prod_sale_fixed_amt': '销售码洋',
    'num': '排行'
}
mayang_yunying_dict={
        'supplier_num':'供应商编码',
        'supplier_name':'供应商名称',
        'prod_sale_fixed_amt': '销售码洋',
        'return_rate': '退货率',
        'zhouzhuan_days': '周转天数',
       'sku_rate': '动销率',
        'amao_rate': '毛利率A',
        'qimo_sku_num': '期末在库品种数',
         'PROD_STOCK_COST_AMOUNT': '期末可销售库存成本',
         'duanhuo_rate': '全店售断率'
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
    for ele in hive_sql:
        a=[column.strip(' ') for column in ele.strip('\n').split('|') if column != '']
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
                key = key + '_' + warehouse_name
            if data.__contains__('product_id'):
                product_id = data.pop('product_id')
                key=key + '_' + product_id

            each_patch_data[key] = data
        i += step

        mysql_data = crm_sql_data(each_patch_data.keys(), tableinfo_key, mysql_cursor, mysql_table, date, month)
        diffkey = util.simplediff(each_patch_data, mysql_data)
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


    hive_sql=_sql.get_crm_product_month_year_top_sql(date,month)
    #每个分组取top N
    # N=3
    # hive_sql="select * from ("+hive_sql+") t where rank<="+str(N)

    #hive_file ,通过文件
    with open('./loadfile/product_month.txt','r') as hive_sql:
        hive_mysql_diff(logger,hive_sql,mysql_table,product_dict.keys(),date,month)

def supply_warehose_rank(date):
    '''断货'''
    mysql_table='crm_supply_warehouse_month_top'
    logger = log.set_logger('warehouse_month_top.txt')

    # hive_sql=_sql.get_crm_warehouse_month_top_sql(date)
    with open('./loadfile/warehouse_month.txt', 'r') as hive_sql:
        hive_mysql_diff(logger, hive_sql, mysql_table,warehouse_dict.keys(), date, True)

    pass

def supply_mayang_yunying(date):
    mysql_table = 'crm_supply_mayang_yunying_month'
    logger = log.set_logger('mayang_yunying.txt')

    hive_sql = _sql.get_crm_mayang_yunying_sql(date)
    hive_mysql_diff(logger, hive_sql, mysql_table, warehouse_dict.keys(), date, True)
    pass


def crm_test():
    data_date="2019-03-01"

    #月
    # product_rank_month_year(data_date)
    #年
    # product_rank_month_year(data_date,False)
    #断货
    supply_warehose_rank(data_date)