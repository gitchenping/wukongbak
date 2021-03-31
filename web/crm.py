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
from utils import util
from utils import log
from .sql import sqldata,crm_sql_data
from utils.util import connect_mysql_from_jump_server
server, mysql_cursor = connect_mysql_from_jump_server('myBdataSupplierDB.db', 'BdataSupplie_rw', 'my@#6rnY9nGQRW', "BdataSupplierDB")
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

# hive_cursor= util.connect_hive()


#品排行 月

def product_rank_month(date):
    logger=log.set_logger('product_rank_month.txt')

    hive_table="dm_dws.dm_order_send_detail"
    mysql_table="crm_supply_product_month_top"

    hive_column_date="concat(substr('{}',0,7),'-01')".format(date)
    hive_column_base="supplier_num,supplier_name," \
            "standard_id,product_id,product_name," \
            "category_path2,path2_name," \
            "original_price"
    hive_column_sale="prod_sale_qty,round(prod_sale_fixed_amt,2)"
    hive_column_rank="Row_number() OVER(partition BY supplier_num,supplier_name " \
                    "ORDER BY prod_sale_fixed_amt desc,prod_sale_qty desc) AS rank"

    hive_sub_where=" data_date >= concat(substr('{}',0,7),'-01') and data_date <= concat(substr('{}',0,7),'-31')".format(date,date)
    hive_sub_where+=" and (supplier_num is not null and supplier_num <> '')" \
                    " and (supplier_name is not null" \
                    " and supplier_name <> '')" \
                    " and (standard_id is not null" \
                    " and standard_id <> '')"
    hive_sub_where+=" and substr(send_date,0,7) = substr('{}',0,7)".format(date)

    hive_sub_groupby=hive_column_base
    hive_sub_table = "select " + hive_column_base + ","+" Sum(prod_sale_qty) AS prod_sale_qty,Sum(prod_sale_fixed_amt) AS prod_sale_fixed_amt"+ \
                    " from "+hive_table+" where "+hive_sub_where+" group by "+hive_sub_groupby

    hive_sql=" select "+hive_column_date+","+hive_column_base+","+hive_column_sale+","+hive_column_rank+ \
        " from ("+hive_sub_table+")t"

    # hive_cursor.execute(hive_sql)
    # hive_data=hive_cursor.fetchmany(size=10000)
    hive_data=[('2020-02-01', '0014001', '山东山大图书有限公司', '9787560756899', '24175032', '学科与专业选修指南：根据教育部高校本科、高职（高专）专业目录编注',
  '01.30.00.00.00.00', '社会科学', 58.0, 1, 58.0, 1), (
 '2020-02-01', '0014001', '山东山大图书有限公司', '9787560755571', '24160936', '折子戏', '01.05.00.00.00.00', '文学', 42.0, 1, 42.0,
 2), (
 '2020-02-01', '0014001', '山东山大图书有限公司', '9787560753478', '23895700', '光信息专业综合实验', '01.63.00.00.00.00', '工业技术', 15.0, 2,
 30.0, 3), (
 '2020-02-01', '0014001', '山东山大图书有限公司', '9787560752297', '23663320', '货运运输组织', '01.25.00.00.00.00', '经济', 21.0, 1, 21.0,
 4), ('2020-02-01', '0014024', '湖北教育出版社有限公司', '9787535178299', '22854255', '朱自清散文集（大阅读·世界文学名著系列·N+1分级阅读丛书)',
      '01.43.00.00.00.00', '中小学教辅', 16.0, 1010, 16160.0, 1), (
 '2020-02-01', '0014024', '湖北教育出版社有限公司', '9787535178152', '22854271', '狐狸列那的故事 彩图注音版（大阅读-语文）', '01.43.00.00.00.00',
 '中小学教辅', 16.8, 642, 10785.6, 2), (
 '2020-02-01', '0014024', '湖北教育出版社有限公司', '9787556428328', '26922942', '学英语 讲中国故事', '01.43.00.00.00.00', '中小学教辅', 236.0,
 32, 7552.0, 3), (
 '2020-02-01', '0014024', '湖北教育出版社有限公司', '9787556411184', '27870988', '奥数思维训练六年级', '01.43.00.00.00.00', '中小学教辅', 38.0,
 97, 3686.0, 4), (
 '2020-02-01', '0014024', '湖北教育出版社有限公司', '9787556411191', '27870985', '奥数思维训练五年级', '01.43.00.00.00.00', '中小学教辅', 38.0,
 86, 3268.0, 5), (
 '2020-02-01', '0014024', '湖北教育出版社有限公司', '9787556411221', '27870987', '奥数思维训练四年级', '01.43.00.00.00.00', '中小学教辅', 38.0,
 72, 2736.0, 6)]

    #分批次查询
    i=5
    step=10
    cover=int(step/2)
    while i<1000:
        each_patch_data={}
        datalist=hive_data[i:i + cover]
        if datalist==[]:
            break
        for each in datalist:
            data=dict(zip(product_dict,each[1:]))
            supplier_num=data.pop('supplier_num')
            product_id=data.pop('product_id')
            each_patch_data[supplier_num+'_'+product_id]=data
        i+=step

        mysql_data=crm_sql_data(each_patch_data.keys(),product_dict.keys(),mysql_cursor,mysql_table,date)
        diffkey=util.simplediff(each_patch_data,mysql_data)
        if diffkey!={}:
            # logger.info('筛选条件: ' + str(filter) + "-*-Fail-*-")
            logger.info("diff info: "+str(diffkey))
            logger.info('')

        return diffkey







    pass

def product_rank_year(date):

    pass




def crm_test():
    data_date="2020-02-01"

    #
    product_rank_month(data_date)