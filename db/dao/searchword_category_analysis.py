'''
搜索词分类分析 ck sql
'''
from utils.db import PyCK
import pandas as pd



#sql
#目标表信息
dev_info ={
    'table':'dm_report.search_order_word_detail',
    'columns_map':{
'日期':"data_date",
          '平台':"case t.platform when '2' then '安卓' when '3' then 'IOS' else '全部' end as platform",
          '一级品类':'path1_name',
          '二级品类':'path2_name' ,
          '三级品类':'path3_name',
          '经营方式':"case t.product_is_merchant when 1 then '自营' when 2 then '招商' else '全部' end as product_is_merchant ",
         '供应商编码/店铺ID':'provid_id' ,
        '搜索词':'search_word',
        '商品曝光PV':'product_pv' ,
        '当天整体搜索PV':'search_all_pv',
        '当天整体搜索UV':'search_all_uv',
        '搜索次数':'search_times' ,
        '搜索PV':'search_pv',
        '搜索UV':'search_uv',
       '点击次数' :'click_times' ,
        '点击UV':'click_uv',
        '搜索UV点击率': "case when search_uv <>0 then round(click_uv / search_uv,2) else null end as click_search_ratio",
'收订用户数':'create_cust_num',
'收订用户转化率':"case when search_uv <>0 then round(create_cust_num / search_uv,2) else null end as create_cust_ratio",
'收订单量':'create_parent_num' ,
        '收订金额' : 'create_sale_amt',
'商品收订件数':'create_quantity_num',
'搜索无结果次数':'no_search_times',
'无结果搜索次数占比':"case when t.search_times <>0 then round(no_search_times / search_times,2) else null end as no_search_times_ratio",
'曝光最大位置中位数':'median_sku_max_ex_location',
'点击最大位置中位数':'median_sku_max_cl_location',
'平均商品曝光最大位置':'avg_sku_max_ex_location',
'平均商品点击最大位置':'avg_sku_max_cl_location' ,
  '跳出率':"case when search_pv <>0 then round(search_no_click / search_pv,2) else null end as jump_over_ratio" ,
"RPM":"case when search_times <>0 then round(create_sale_amt / search_times,2)*1000 else null end as rpm",
        '搜索UV价值':"case when search_uv <>0 then round(create_sale_amt / search_uv,2) else null end as search_uv_value",
'支付金额':'zf_prod_sale_amt' ,
'实付金额':'out_pay_amount'
    },
    # 'src_conn': PyHive(),
    'dst_conn': PyCK()
}




def get_sql(data,module:str):
    '''

    :param data:
    :param module:
    :return:
    '''

    filters = data['filters']
    params = data['params']
    params_dict = {ele['name']: ele['value'].strip("'") for ele in params}

    if filters !=[]:
        searchword = filters[0]['value']
        params_dict.update({'search_word': searchword})

    # event_id IN (4311, 4201, 4031, 6403) AND LENGTH(search_word) > 0
    start_time = params_dict['start']
    end_time = params_dict['end']

    where = " data_date between '" + start_time + "' and '" + end_time + "' "

    if params_dict.__contains__('platform_name'):
        platform_name = params_dict['platform_name']
        app_id = get_platform_id(platform_name)

        where += " and t.platform = '" + str(app_id) + "'"

    path1_name = "全部"
    if params_dict.__contains__('path1_name'):
        path1_name = params_dict['path1_name']
    where += " and path1_name='" + path1_name + "'"

    path2_name = "全部"
    if params_dict.__contains__('path2_name'):
        path2_name = params_dict['path2_name']
    where += " and path2_name='" + path2_name + "'"

    path3_name = "全部"
    if params_dict.__contains__('path3_name'):
        path3_name = params_dict['path3_name']
    where += " and path3_name='" + path3_name + "'"

    if params_dict.__contains__('product_is_merchant_name'):
        is_merchant = params_dict['product_is_merchant_name']
        is_merchant_id = get_shop_type(is_merchant)
        where += " and t.product_is_merchant = " + str(is_merchant_id)

    if params_dict.__contains__('provid_id'):
        provid_id = params_dict['provid_id']

        where += " and provid_id = '" + provid_id + "'"

    if params_dict.__contains__('search_word'):
        search_word = params_dict['search_word']

        where += " and search_word = '" + search_word+"'"

    #拼sql
    sql =" select {column_str} from {table} t where {where} {group_by} {order_by}"

    weidu: list = data['groups']
    weidu_en = [dev_info['columns_map'][ele] for ele in weidu]
    group_by = " group by "+ ','.join([dev_info['columns_map'][ele] for ele in weidu])
    order_by =''

    df_column = weidu + ['sum(' + ele['column'] + ")" for ele in data['aggregators']]

    column_str = ','.join(weidu_en+['sum('+dev_info['columns_map'][ele['column']]+")" for ele in data['aggregators']])

    if module.__contains__('top'):

        order_by =" order by sum(search_times) desc limit 1000"

    ck_sql = sql.format(column_str=column_str, table=dev_info['table'], where=where,group_by = group_by,order_by=order_by)

    return ck_sql,df_column,params_dict



def get_sql_data(data,module):
    '''

    :param data: 筛选入参
    :return:
    '''

    sql,table_column,params_dict = get_sql(data,module)
    print(sql)
    ck_hd = dev_info['dst_conn']

    sql_data_list = ck_hd.get_result_from_db(sql)

    df = pd.DataFrame(sql_data_list,columns = table_column)

    return df,params_dict

