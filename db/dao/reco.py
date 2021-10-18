'''
北极星推荐报表-单品分析sql
dm_report.reco_order_detail ->tp_reco_kpi_product_1\tp_reco_kpi_product_2  -> dm_report.dm_reco_kpi_product
'''
import pandas as pd
import numpy as np

#维表
app_id={'全部':"('2','3')",'IOS':"('3')",'安卓':"('2')"}
product_is_merchant={'自营':'(1)','招商':'(2)','全部':'(1,2)'}

bd_id={'出版物':'(1)','日百服':'(2)','数字':'(3)','文创':'(4)','其他':'(5)','全部':'(1,2,3,4,5)'}

indicator_cal_map={
    '商品ID':'product_id',
    '商品曝光pv':"COUNT(CASE WHEN model_type = 1 THEN udid ELSE null END) AS product_expose_pv",
    '商品曝光uv':"COUNT(DISTINCT CASE WHEN model_type = 1 THEN udid ELSE NULL END) AS product_expose_uv",
    '商品点击pv':"COUNT(DISTINCT CASE WHEN model_type = 3 THEN concat(udid,creation_date,toString(main_product_id)) ELSE NULL END) AS product_click_pv",
    '商品点击uv':"COUNT(DISTINCT CASE WHEN model_type = 3 THEN udid ELSE NULL END) AS product_click_uv",
    '商品UV点击率':'case when product_expose_uv>0 then round(product_click_uv / product_expose_uv*100,2) else null end as uv_ctr',
    '商品PV点击率':'case when product_expose_pv>0 then round(product_click_pv / product_expose_pv*100,2) else null end as pv_ctr',
    '原始订单行数':'COUNT(CASE WHEN order_id = -1 THEN NULL ELSE order_id END) AS create_hang_num',
    '收订单量':'COUNT(DISTINCT CASE WHEN order_id = -1 THEN NULL ELSE order_id END) AS create_parent_num',
    '收订金额':'SUM(bargin_price * order_quantity) AS create_sale_amt',
    '收订顾客数':'COUNT(DISTINCT CASE WHEN order_id = -1 THEN NULL ELSE order_cust_id END) AS create_cust_num',
    '收订转化率':'case when product_click_pv >0 then round(create_hang_num / product_click_pv*100,2) else null end as create_trans_rate',
    '最大曝光位置':'MAX(CASE WHEN model_type = 1 AND position > 0 THEN position ELSE 0 END) AS max_expose_location',
    # '平均曝光位置':'ROUND(AVG(CASE WHEN model_type = 1 AND `position` > 0 THEN `position` ELSE null END)) AS avg_expose_deepth',
    '最大点击位置':'MAX(CASE WHEN model_type = 3 AND position > 0 THEN position ELSE 0 END) AS max_click_location',
    # '平均点击位置':'ROUND(AVG(CASE WHEN model_type = 3 AND `position` > 0 THEN `position` ELSE null END)) AS avg_click_deepth',
    '人均点击次数':'case when product_click_uv>0 then round(product_click_pv / product_click_uv,2) else null end as avg_click_num'
}

def get_filters_where_for_reco(filters):
    '''推荐-单品分析筛选条件'''

    wheredict = dict(filters)
    where = ''
    #商品ID
    if wheredict.__contains__('商品ID'):
        where += ' product_id = '+ str(wheredict['商品ID'])+" and "

    #日期
    date_str_start = wheredict['start']
    date_str_end = wheredict['end']
    where += "data_date between'" + date_str_start + "'"+" and '"+date_str_end+"' and "
    #平台
    appid = " in "+app_id[wheredict['platform']]
    where += "app_id "+ appid+ " and "
    #经营方式
    shoptype = " in "+product_is_merchant[wheredict['经营方式']]
    where += "product_is_merchant" + shoptype + " and "
    #事业部
    bd = " in " +bd_id[wheredict['bd_id']]
    where += "bd_id "+bd+' and '
    #二级品类

    path2_name=wheredict['path2_name']
    if path2_name!='全部':
        where+="path2_name='"+path2_name+"'"+" and "

    #页面
    page_name = wheredict['page_name']

    if page_name!='全部':
        where += "model_page_name='"+page_name+"'"+" and "

    #模块
    module_name = wheredict['model_cn_name']
    if module_name != '全部':
        where += "model_cn_name='"+module_name+"'"+ " and "

    return where.rstrip('and ')
    pass

'''推荐sql data'''
def get_sql_data_reco(filter,conn_ck=None):
    # 获取筛选条件
    where = get_filters_where_for_reco(filter)
    groupby =  " group by product_id"

    # sql
    column_all = ''
    for _, column in indicator_cal_map.items():
        column_all += column + ","
    select_column = column_all.strip(',')

    base_sql = "select " + select_column + " from dm_report.reco_order_detail " + " where " + where+ groupby

    # conn_ck.execute(sql)
    # ck_data = conn_ck.fetchall()
    #
    # df = pd.DataFrame(ck_data, columns=indicator_cal_map.keys())


     # 平均曝光、平均点击
    expose_click_column = "product_id," \
                          "ROUND(AVG(CASE WHEN expose_location = -1 THEN NULL ELSE expose_location END)) as avg_expose_deepth," \
                          "ROUND(AVG(CASE WHEN click_location = -1 THEN NULL ELSE click_location END)) AS avg_click_deepth"
    expose_click_inner_sql = '''
    SELECT
	app_id AS platform,
	product_is_merchant AS shop_type,
	bd_id,
	category_path2,
	path2_name,
	model_page_id AS page_id,
	model_page_name AS page_name,
	model_cn_name,
	product_id,
	product_name,
	udid,
	MAX(CASE WHEN model_type = 1 THEN `position` ELSE -1 END) AS expose_location,
	MAX(CASE WHEN model_type = 3 THEN `position` ELSE -1 END) AS click_location
	FROM
	dm_report.reco_order_detail
    WHERE
	{where}
    GROUP BY
	app_id,
	product_is_merchant,
	bd_id,
	category_path2,
	path2_name,
	model_page_id,
	model_page_name,
	model_cn_name,
	product_id,
	product_name,
	udid
    '''

    expose_click_sql = " select " + expose_click_column + " from (" + expose_click_inner_sql.format(where = where) + ") a group by product_id"

    sql ="select t1.*,t2.avg_expose_deepth,t2.avg_click_deepth from ("+ base_sql+")t1 left join ("+expose_click_sql+")t2 on t1.product_id = t2.product_id"

    conn_ck.execute(sql)
    ck_data = conn_ck.fetchall()

    column_list = list(indicator_cal_map.keys())+['平均曝光位置','平均点击位置']
    df = pd.DataFrame(ck_data, columns = column_list)

    return df







