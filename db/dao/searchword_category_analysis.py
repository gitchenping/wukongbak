'''
�����ʷ������ ck sql
'''
from utils.db import PyCK
import pandas as pd



#sql
#Ŀ�����Ϣ
dev_info ={
    'table':'dm_report.search_order_word_detail',
    'columns_map':{
'����':"data_date",
          'ƽ̨':"case t.platform when '2' then '��׿' when '3' then 'IOS' else 'ȫ��' end as platform",
          'һ��Ʒ��':'path1_name',
          '����Ʒ��':'path2_name' ,
          '����Ʒ��':'path3_name',
          '��Ӫ��ʽ':"case t.product_is_merchant when 1 then '��Ӫ' when 2 then '����' else 'ȫ��' end as product_is_merchant ",
         '��Ӧ�̱���/����ID':'provid_id' ,
        '������':'search_word',
        '��Ʒ�ع�PV':'product_pv' ,
        '������������PV':'search_all_pv',
        '������������UV':'search_all_uv',
        '��������':'search_times' ,
        '����PV':'search_pv',
        '����UV':'search_uv',
       '�������' :'click_times' ,
        '���UV':'click_uv',
        '����UV�����': "case when search_uv <>0 then round(click_uv / search_uv,2) else null end as click_search_ratio",
'�ն��û���':'create_cust_num',
'�ն��û�ת����':"case when search_uv <>0 then round(create_cust_num / search_uv,2) else null end as create_cust_ratio",
'�ն�����':'create_parent_num' ,
        '�ն����' : 'create_sale_amt',
'��Ʒ�ն�����':'create_quantity_num',
'�����޽������':'no_search_times',
'�޽����������ռ��':"case when t.search_times <>0 then round(no_search_times / search_times,2) else null end as no_search_times_ratio",
'�ع����λ����λ��':'median_sku_max_ex_location',
'������λ����λ��':'median_sku_max_cl_location',
'ƽ����Ʒ�ع����λ��':'avg_sku_max_ex_location',
'ƽ����Ʒ������λ��':'avg_sku_max_cl_location' ,
  '������':"case when search_pv <>0 then round(search_no_click / search_pv,2) else null end as jump_over_ratio" ,
"RPM":"case when search_times <>0 then round(create_sale_amt / search_times,2)*1000 else null end as rpm",
        '����UV��ֵ':"case when search_uv <>0 then round(create_sale_amt / search_uv,2) else null end as search_uv_value",
'֧�����':'zf_prod_sale_amt' ,
'ʵ�����':'out_pay_amount'
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

    path1_name = "ȫ��"
    if params_dict.__contains__('path1_name'):
        path1_name = params_dict['path1_name']
    where += " and path1_name='" + path1_name + "'"

    path2_name = "ȫ��"
    if params_dict.__contains__('path2_name'):
        path2_name = params_dict['path2_name']
    where += " and path2_name='" + path2_name + "'"

    path3_name = "ȫ��"
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

    #ƴsql
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

    :param data: ɸѡ���
    :return:
    '''

    sql,table_column,params_dict = get_sql(data,module)
    print(sql)
    ck_hd = dev_info['dst_conn']

    sql_data_list = ck_hd.get_result_from_db(sql)

    df = pd.DataFrame(sql_data_list,columns = table_column)

    return df,params_dict

