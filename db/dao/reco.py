

app_id={'全部':"('2','3')",'IOS':"('2')",'安卓':"('3')"}
product_is_merchant={'自营':'(1)','招商':'(2)','全部':'(1,2)'}

bd_id={'出版物':'(1)','日百服':'(2)','数字':'(3)','文创':'(4)','其他':'(5)','全部':'(1,2,3,4,5)'}

indicator_cal_map={
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
    '收订转化率':'case when product_expose_uv>0 then round(create_cust_num / product_expose_uv*100,2) else null end as create_trans_rate',
    '最大曝光位置':'MAX(CASE WHEN model_type = 1 AND position > 0 THEN position ELSE 0 END) AS max_expose_location',
    # '平均曝光位置':'ROUND(AVG(CASE WHEN model_type = 1 AND `position` > 0 THEN `position` ELSE null END)) AS avg_expose_deepth',
    '最大点击位置':'MAX(CASE WHEN model_type = 3 AND position > 0 THEN position ELSE 0 END) AS max_click_location',
    # '平均点击位置':'ROUND(AVG(CASE WHEN model_type = 3 AND `position` > 0 THEN `position` ELSE null END)) AS avg_click_deepth',
    '人均点击次数':'case when product_click_uv>0 then round(product_click_pv / product_click_uv,2) else null end as avg_click_num'
}

def get_filters_where_for_reco(filters):
    '''推荐-单品分析筛选条件'''

    wheredict=dict(filters)
    where=''
    #日期
    data_date=wheredict['start']
    where += "data_date='" + data_date + "'"+" and "
    #平台
    appid=" in "+app_id[wheredict['platform']]
    where+="app_id "+ appid+ " and "
    #经营方式
    shoptype=" in "+product_is_merchant[wheredict['shop_type_name']]
    where+="product_is_merchant"+shoptype+ " and "
    #事业部
    bd =" in " +bd_id[wheredict['bd_id']]
    where+="bd_id "+bd+' and '
    #二级品类

    path2_name=wheredict['path2_name']
    if path2_name!='全部':
        where+="path2_name='"+path2_name+"'"+" and "

    #页面
    page_name=wheredict['page_name']

    if page_name!='全部':
        where+="model_page_name='"+page_name+"'"+" and "

    #模块
    module_name=wheredict['model_cn_name']
    if module_name != '全部':
        where+="model_cn_name='"+module_name+"'"+ " and "

    return where.rstrip('and ')
    pass

'''推荐sql data'''
def get_sql_data_reco(data,filters,conn_ck=None):
    # 获取筛选条件
    where = get_filters_where_for_reco(filters)
    if data!={}:
        date=data['日期']
        product_id = data['商品ID']
        product_name = data['商品名称']
        where += ' and product_id= ' + str(product_id) \
                 #+ " and product_name= '" + str(product_name) + "'"
    else:
        where+=" and product_id != -1 group by product_id"

    # sql
    column_all = ''
    for _, column in indicator_cal_map.items():
        column_all += column + ","
    final_column = column_all.strip(',')

    sql = "select " + final_column + " from dm_report.reco_order_detail " + " where " + where

    conn_ck.execute(sql)
    ck_data = conn_ck.fetchall()

    sqldata={}
    if len(ck_data)>0:
        sqldata=dict(zip(indicator_cal_map.keys(),ck_data[0]))

        if sqldata['商品曝光pv']==0 and sqldata['商品点击pv']==0 and sqldata['收订金额']==0:
            return {}
        if data=={}:
            return sqldata

        # 平均曝光、平均点击
        expose_click_column = "count(DISTINCT CASE WHEN model_type = 1 THEN udid ELSE NULL END) as expose_uv," \
                              "count(DISTINCT CASE WHEN model_type = 3 THEN udid ELSE NULL END) as click_uv," \
                              "round(case when expose_uv!=0 then sum(max_expose_location) /expose_uv else null end) as avg_expose_location," \
                              "round(case when click_uv!=0 then sum(max_click_location) /click_uv else null end) as avg_click_location"
        expose_click_inner_sql = " select model_type,udid,MAX(CASE WHEN model_type = 1 then position else 0 end) AS max_expose_location," \
                                 "MAX(CASE WHEN model_type = 3 then position else 0 end) AS max_click_location from dm_report.reco_order_detail" \
                                 " where " + where + " and model_type in (1,3) and position>0 group by udid,model_type"

        expose_click_sql = " select " + expose_click_column + " from (" + expose_click_inner_sql + ") a"

        conn_ck.execute(expose_click_sql)
        ck_data = conn_ck.fetchall()
        if len(ck_data) > 0:
            sqldata['平均曝光位置'] = ck_data[0][-2]
            sqldata['平均点击位置'] = ck_data[0][-1]

    if data!={}:
        sqldata['日期']=date
        sqldata['商品ID']=product_id
        sqldata['商品名称']=product_name

    return sqldata







