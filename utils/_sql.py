from .date import get_tb_hb_date
from .tb_hb import tb_hb_cal,get_tb_hb_key

def get_platform_where(data,yinhao=False):
    '''获取platform对应的过滤条件'''

    source = data['source']
    parentplatform = data['parent_platform']
    platform = data['platform']
    #
    if yinhao:          #默认不加
        _yin="'"
    else:
        _yin=""

    sourcewhere = ''
    if source == 'all':
        sourcewhere = "("+_yin+'1'+_yin+','+_yin+'2'+_yin+','+_yin+'3'+_yin+","+_yin+'4'+_yin+")"
    else:
        sourcewhere = "(" + _yin+source+_yin + ")"
    #
    platformwhere = ''
    if source == '1' and parentplatform == '3':
        platformwhere = "("+_yin+'12'+_yin+','+_yin+'20'+_yin+")"
    elif source == '1' and parentplatform == '4':
        platformwhere = "("+_yin+'0'+_yin+")"
    elif source == '1' and parentplatform == '1' and platform == 'all':  #
        platformwhere = "("+_yin+'1'+_yin+','+_yin+'2'+_yin+")"
    elif source == '1' and parentplatform == '2' and platform == 'all':
        platformwhere = "("+_yin+'3'+_yin+','+_yin+'4'+_yin+','+_yin+'5'\
        +_yin +','+_yin+'6'+_yin+','+_yin+'7'+_yin+','+_yin+'8'+_yin+','+_yin+'9'+_yin+")"
    elif source=='1' and parentplatform=='all':
        platformwhere = "("+_yin+'0'+_yin+','+ _yin+'1'+_yin+','+_yin+'2'+_yin+ ','+_yin + '3' +\
                        _yin + ',' + _yin + '4' + _yin + ',' + _yin + '5' \
                        + _yin + ',' + _yin + '6' + _yin + ',' + _yin + '7' + _yin + ',' \
                        + _yin + '8' + _yin + ',' + _yin + '9' + _yin +','+ _yin+'12'+_yin+','+_yin+'20'+_yin+")"
    else:
        platformwhere = "(" +_yin+ platform +_yin+ ")"

    if source=='1':
        platformwhere=" and platform in "+platformwhere
    else:
        platformwhere = ''

    return " source in "+sourcewhere+platformwhere


def is_platform_show(data):
    '''下钻页平台是否显示'''
    datacopy=dict(data)

    source = datacopy['source']
    parentplatform = datacopy['parent_platform']
    platform = datacopy['platform']

    show=True                              #默认显示
    if source not in ['2', '3', '4']:                       # 天猫、抖音、拼多多下钻页没有平台分布
        if parentplatform not in ['2', '3', '4']:           # 轻应用\H5\PC没有平台分布

            if parentplatform != 'all':
                if platform =='all':          # 安卓、IOS没有平台分布
                    show=True
                else:
                    show=False
            else:
                show=True
        else:
            show = False
    else:
        show= False
    return show


def get_bd_where(data):
    '''获取bd对应的过滤条件'''
    bd = data['bd_id']
    if bd == '1':                    #出版物
        bd_id = '(5,12)'
    elif bd == '2':                 #百货
        bd_id = '(1,4,9,15,16)'
    elif bd == '3':                 #数字
        bd_id = '(6)'
    elif bd == '4':                 #文创
        bd_id = '(20,21,23)'
    elif bd=='5':                   #其他
        bd_id = '(0,13,19)'
    elif bd=='6':                   #服装
        bd_id = '(3)'
    else:
        bd_id = 'all'
    bdwhere=''
    if bd_id !='all':
        bdwhere=" and bd_id in " +bd_id
    else:
        bdwhere = " and bd_id in " + "( 1, 3, 4, 5,6, 9, 12, 15, 16, 20, 21, 23)"
    return bdwhere

def get_shoptype_where(data):
    '''获取shoptype对应的过滤条件'''
    shoptype= data['shop_type']

    if shoptype == '1':
        shop_type = '(1)'
    elif shoptype == '2':
        shop_type = '(2)'
    else:
        shop_type = 'all'

    shoptypewhere=''
    if shop_type!='all':
        shoptypewhere = " and shop_type in " + shop_type
    return shoptypewhere

def get_eliminate_where(data):
    '''获取shoptype对应的过滤条件'''
    jianggong= data['eliminate_type']

    if jianggong == '1':
        jianggong_type = '(1)'
    elif jianggong == '2':
        jianggong_type = '(1)'
    else:
        jianggong_type = 'all'

    jianggongwhere=''
    if jianggong_type!='all':
        jianggongwhere = " and is_jiangong in " + jianggong_type
    return jianggongwhere

'''返回sql筛选的时间（环比、同比）条件'''
def get_time_where(data):
    '''
    :param data: 字典
    :return:
    '''
    datetype=data['date_type']
    date=data['date_str']

    datewhere=''
    tb_hb_date = get_tb_hb_date(date, datetype)

    if len(tb_hb_date)==4:     #天返回环比、周同比、年同比

        today=tb_hb_date[0]
        yesterday=tb_hb_date[1]
        last_week_day=tb_hb_date[2]
        last_year_day=tb_hb_date[3]

        datewhere += ' and date_str in ' + "('" + today+"','"+yesterday+"','"+last_week_day+"','"+last_year_day+"')"

    else:      #周、月、季返回环比、同比
        hb_date=tb_hb_date[0]
        tb_date=tb_hb_date[1]

        for ele in tb_hb_date:
            if ele[0] is not None:
                 datewhere+= " date_str between '"+ele[0]+"' and '"+ele[1]+"' or "
        datewhere=" and ("+datewhere.strip('or ')+" )"
    return datewhere

'''下钻页各指标同比环比计算'''
def get_drill_tb_hb(ck_data,name_dict,date,datetype,misskeyshow=True,misskeyvalue='--'):
    '''
    :param ck_data: 二维列表,第一列作为下钻分类
    :param name_dict: 字典
    :param date: 时间字符串
    :param datetype: 时间类型
    :return: 同环比
    '''
    tempdict={}
    temp = []
    i = 0
    begin_index = ck_data[i][0]

    is_need_cal = False
    length = len(ck_data)
    while i <= length - 1:

        if begin_index == ck_data[i][0]:
            temp.append(ck_data[i][1:])  # 取值和时间
            key = str(begin_index)
            i += 1
        else:
            begin_index = ck_data[i][0]
            is_need_cal = True

        if i == length:
            is_need_cal = True

        if is_need_cal:
            # 计算环比、同比
            tb_hb_key_list,default_tb_hb_key_list = get_tb_hb_key(temp, date, datetype)

            if len(tb_hb_key_list) > 0:  # 可以进行同环比计算
                newdata = tb_hb_cal(temp,misskeyvalue)

                temp_2 = []
                for col in range(len(newdata[0])-1):

                    for raw in newdata:
                        temp_2.append(raw[col])

                name = name_dict[key]
                tempdict[name] = dict(zip(tb_hb_key_list, temp_2))

                #没有的key，值设置为'--'
                if misskeyshow:
                    nokey=set(default_tb_hb_key_list)-set(tb_hb_key_list)
                    if len(nokey)>0:
                        for ele in nokey:
                            tempdict[name].update({ele:misskeyvalue})
                #
                temp = []
                is_need_cal = False
            else:
                temp=[]
                is_need_cal = False
    return tempdict

def get_where_for_report(data,uv=True):
    '''实时报表字段、筛选条件'''
    category=data['categoryPath']
    category_list=[ele  for ele in category.split('.') if ele!='00']
    cate_len=len(category_list)
    if cate_len==1:
        category_path = "category_path1"
        column_category="category_path2"
    elif cate_len==2:
        category_path = "category_path2"
        column_category = "category_path3"
    else:
        category_path = "category_path3"
        column_category = "category_path3"

    where=''

    date_str = data['queryDate']
    date_str_list = date_str.split('-')
    date_str_list[0] = str(int(date_str_list[0]) - 1)
    date_str_2 = '-'.join(date_str_list)
    where += " date_str in ('" + date_str + "'," + "'" + date_str_2 + "')"

    where += " and " + category_path + "='" + category + "'"

    if data.__contains__('endTime') and data['endTime'] != '':
        hour_str = data['endTime'].split(':')[0]
        where += " and hour_str <" + "'" + hour_str + "'"

    source=data['source']
    if source!='0':
        where+=' and source='+"'"+source+"'"

    if uv:
        fromplatform=data['fromPlatform']
        if fromplatform not in ['']:
            temp = ','.join(["'" + str(ele) + "'" for ele in fromplatform.split(',')])
            where+=' and from_platform in ('+temp+")"

    else:
        platform=data['platform']
        if platform not in ['']:
            temp=','.join(["'"+str(ele)+"'" for ele in platform.split(',')])
            where+=' and platform in ('+temp+")"

    shop_type=str(data['mgtType'])
    if shop_type!='0':
        where+=" and shop_type= '"+shop_type+"'"

    if uv:
        bd_id_prod = get_bd_where({'bd_id': str(data['bizType'])})
        if len(bd_id_prod) > 0:
            bd_id_prod = bd_id_prod.replace('bd_id', 'bd_id_prod')
            where+=bd_id_prod
    else:
        if data['bizType'] != 0:
            where += " and bd_id ='" + str(data['bizType']) + "'"


    return where,column_category


def get_uv_sql_for_report(data,tablename,reportname='category'):
    '''实时报表-uv'''
    where,column_category=get_where_for_report(data)

    if reportname=='category':
        if 'category_path2' not in where:
            uvwhere = where
        else:
            uvwhere=where+" and category != '"+data['categoryPath']+"'"+" and category !='' "
    else:
        uvwhere = where

    column=''
    orderbyname=''
    if reportname=='bussiness':
        orderbyname="_bd_id"
        column="case when bd_id_prod in (5,12) then '1'" \
               " when bd_id_prod in (1,4,9,15,16) then '2'" \
               " when bd_id_prod in (3) then '6'" \
               " when bd_id_prod in (20,21,23) then '4'" \
               " when bd_id_prod in (6) then '3'" \
               " else '5' end "+ " as "+orderbyname
    else:
        orderbyname="category"
        column=column_category+" as "+orderbyname
    column+=',COUNT(DISTINCT device_id) AS UV,toString(date_str) as date_str'

    order_by=" order by "+ orderbyname+",date_str desc"

    sql=" select "+column+" from "+tablename+" where "+uvwhere+" group by "+orderbyname+",date_str "+order_by
    return sql

sd_zf_report_map={
    #指标
    'sd_zf_amount':"SUM(case when data_type='{}'then bargin_price * order_quantity else null end) AS amount",   #金额
    'sd_zf_package':"COUNT(DISTINCT case when data_type='{}'then order_id else null end) AS package",     #包裹量
    'sd_zf_customer':"COUNT(DISTINCT case when data_type='{}'then cust_id else null end) AS customer",       #人数
    'sd_zf_new_customer':"COUNT(DISTINCT CASE WHEN data_type='{}' and new_id = '1' then cust_id else null end) AS new_customer" #新客
}

def get_sd_info_sql_for_report(data,tablename,reportname):
    '''实时报表收订-其他指标'''
    where, column_category = get_where_for_report(data,False)

    where += " and data_type in ('1','4')"


    if reportname!='bussiness':       #品类
        if 'category_path2' not in where:
            sdwhere = where
        else:
            sdwhere = where + " and category != '" + data['categoryPath'] + "'" + " and category !='' "

        sd_zf_category = column_category + " as category"
        column=sd_zf_category+","
        groupby_orderby=" group by "+"category,date_str"+" order by category,date_str desc limit 4"
    else:
        sdwhere = where

        column = "bd_id" + ","
        groupby_orderby = " group by " + "bd_id,date_str" + " order by bd_id,date_str desc"

    for cloumn_key in sd_zf_report_map.keys():
        column+=sd_zf_report_map[cloumn_key].format('1')+","

    sd_zf_order="COUNT(DISTINCT case when data_type = '1' then  parent_id else null end) AS order_sum"

    #收订
    sd_cancel = "COUNT(DISTINCT case when data_type='4' then parent_id else null end) AS order_cancel"
    sd_cancel_rate="(case when order_sum!=0 then order_cancel /order_sum *100 else null end ) as sd_cancel_rate"

    column+=sd_cancel_rate+",date_str,"+sd_zf_order+","+sd_cancel

    sd_sql="select "+column+ \
           " from "+tablename+ \
           " where "+sdwhere+groupby_orderby
    return sd_sql


def get_zf_info_sql_for_report(data,tablename,reportname):
    '''实时报表支付-其他指标'''
    where, column_category = get_where_for_report(data,False)

    where += " and data_type in ('2','4')"

    sd_zf_order = "COUNT(DISTINCT case when data_type = '2' then  parent_id else null end) AS order_sum"
    if reportname != 'bussiness':  # 品类
        if 'category_path2' not in where:
            zfwhere = where
        else:
            zfwhere = where + " and category != '" + data['categoryPath'] + "'" + " and category !='' "

        sd_zf_category = column_category + " as category"
        column = sd_zf_category + ","

        for cloumn_key in sd_zf_report_map.keys():
            column += sd_zf_report_map[cloumn_key].format('2') + ","
        column += "date_str," + sd_zf_order

        a_sql = " select " + column + " from " + tablename + " where " + zfwhere + " group by " + "category,date_str"

        b_sql = " select COUNT(DISTINCT parent_id) as cancel_num,date_str,category from (" + \
                "select parent_id,COUNT(DISTINCT data_type ) AS num,date_str," + column_category + " as category" + \
                " from " + tablename + " where " + zfwhere + " group by parent_id,date_str,category having num>1) a" + \
                " group by category,date_str"
        groupbyname = "category"
        groupby_orderby = " order by category,date_str desc limit 4"
    else:
        zfwhere = where
        column = "bd_id" + ","

        for cloumn_key in sd_zf_report_map.keys():
            column += sd_zf_report_map[cloumn_key].format('2') + ","
        column += "date_str," + sd_zf_order

        a_sql = " select " + column + " from " + tablename + " where " + zfwhere + " group by " + "bd_id,date_str"

        b_sql = " select COUNT(DISTINCT parent_id) as cancel_num,date_str,bd_id from (" + \
                "select parent_id,COUNT(DISTINCT data_type ) AS num,date_str," + " bd_id" + \
                " from " + tablename + " where " + zfwhere + " group by parent_id,date_str,bd_id having num>1) a" + \
                " group by bd_id,date_str"
        groupbyname="bd_id"
        groupby_orderby=" order by bd_id,date_str desc"

    zf_sql = "select " +groupbyname+" ,amount,package,customer,new_customer, " + \
            "case when a.order_sum!=0 then b.cancel_num /a.order_sum* 100 else null end as zf_cancel_rate,date_str "+ \
             " from (" +a_sql  +") a left join ("+ \
             b_sql+") b"+" on a."+groupbyname+" = b."+groupbyname+" and a.date_str = b.date_str"+ groupby_orderby

    return zf_sql

def get_crm_product_month_year_top_sql(date,month=True):
    hive_table = "dm_dws.dm_order_send_detail"

    if month:  # 月top

        hive_column_date = date[0:7] + "-01"
        hive_column_datadate_begin = date[0:7] + "-01"
        hive_column_datadate_end = date[0:7] + "-31"

        hive_column_senddate_where = " and substr(send_date,0,7) = '" + date[0:7] + "'"

    else:

        hive_column_date = date[0:4] + "-01-01"
        hive_column_datadate_begin = date[0:4] + "-01-01"
        hive_column_datadate_end = date[0:4] + "-12-31"

        hive_column_senddate_where = " and substr(send_date,0,4) = '" + date[0:4] + "'"

    hive_column_base = "supplier_num,supplier_name," \
                       "standard_id,product_id,product_name," \
                       "category_path2,path2_name," \
                       "original_price"
    hive_column_sale = "prod_sale_qty,round(prod_sale_fixed_amt,2)"
    hive_column_rank = "Row_number() OVER(partition BY supplier_num,supplier_name " \
                       "ORDER BY prod_sale_fixed_amt desc,prod_sale_qty desc) AS rank"

    hive_sub_where = " data_date >= '{}' and data_date <= '{}'".format(hive_column_datadate_begin,
                                                                       hive_column_datadate_end)
    hive_sub_where += " and (supplier_num is not null and supplier_num <> '')" \
                      " and (supplier_name is not null" \
                      " and supplier_name <> '')" \
                      " and (standard_id is not null" \
                      " and standard_id <> '')"
    hive_sub_where += hive_column_senddate_where+ " and  sale_type=1 "

    hive_sub_groupby = hive_column_base
    hive_sub_table = "select " + hive_column_base + "," + " Sum(prod_sale_qty) AS prod_sale_qty,Sum(prod_sale_fixed_amt) AS prod_sale_fixed_amt" + \
                     " from " + hive_table + " where " + hive_sub_where + " group by " + hive_sub_groupby

    hive_sql = " select '" + hive_column_date + "'," + hive_column_base + "," + hive_column_sale + "," + hive_column_rank + \
               " from (" + hive_sub_table + ")t"

    return hive_sql

def get_crm_warehouse_month_top_sql(date):
    data_date_begin=date[0:7]+"-01"
    data_date_end=date[0:7]+"-31"
    send_date=date[0:7]
    trans_date=date[0:7]

    stock_table='''
        SELECT
            warehouse_id,
            d.supplier_code as supplier_id,
            product_id,
            warehouse_name
        FROM(
                SELECT
                    CASE
                        WHEN trim(warehouse_id) = '10' THEN 'shenyang'
                        WHEN trim(warehouse_id) = '15' or trim(warehouse_id) = '27' THEN 'guangzhou'
                        WHEN trim(warehouse_id) = '17' THEN 'wuxi'
                        WHEN trim(warehouse_id) = '20' THEN 'fuzhou'
                        WHEN trim(warehouse_id) = '22' or trim(warehouse_id) = '28' THEN 'jinan'
                        WHEN trim(warehouse_id) = '29' THEN 'beijing'
                        WHEN trim(warehouse_id) = '30' THEN 'tianjin'
                        WHEN trim(warehouse_id) = '39' THEN 'wuhan'
                        WHEN trim(warehouse_id) = '41' or trim(warehouse_id) = '5' THEN 'chengdu'
                        WHEN trim(warehouse_id) = '9' THEN 'zhengzhou'
                        ELSE '0'
                    END AS warehouse_name,
                    supplier_id,
                    warehouse_id,
                    product_id,
		            sum( CASE WHEN ( region_use_id IN ( '2', '3' ) OR region_use_id IN ( '003', '004' ) ) AND ywbmid = '4' THEN physical_stock_quantity 
					          WHEN region_use_id IS NULL AND ywbmid IN( '2', '3', '5' ) THEN zpkc 
					     ELSE 0 END) AS prod_qty_stock_saleable_amount
		            FROM dwd.prod_stock_supp_detail
		            WHERE
                        substr(trans_date,0,7) = '{}'
                        AND warehouse_id IN ('10','15','27','17','20','22','28','29','30','39','41','5','9')
                        AND supplier_id IS NOT NULL
                        AND supplier_id != ''
                    GROUP BY
                        warehouse_id,
                        supplier_id,
                        product_id
        )t
        left join dim.dim_supplier d 
        on t.supplier_id = d.supplier_id
        WHERE t.prod_qty_stock_saleable_amount <= 0
    '''.format(trans_date)

    order_send_detail_table='''
        SELECT 
            supplier_num,
            supplier_name,
            warehouse_id,
            standard_id,
            product_id,
            product_name,
            category_path2,
            path2_name,
            original_price,
            sum(prod_sale_qty) AS prod_sale_qty,
            sum(prod_sale_fixed_amt) AS prod_sale_fixed_amt
	FROM
		    dm_dws.dm_order_send_detail
	WHERE
            data_date >='{}'
            AND data_date <= '{}'
            AND ( supplier_name IS NOT NULL
            AND supplier_name <> '')
            AND substr(send_date,0,7) ='{}'
            and standard_id is not null
            and standard_id <> ''
	GROUP BY
            supplier_num,
            supplier_name,
            warehouse_id,
            standard_id,
            product_id,
            product_name,
            category_path2,
            path2_name,
            original_price
    '''.format(data_date_begin,data_date_end,send_date)

    hive_sql='''
        SELECT '{}', 
            t1.supplier_id, 
            t2.supplier_name, 
            t1.warehouse_id, 
            t1.warehouse_name, 
            t2.standard_id, 
            t1.product_id, 
            t2.product_name, 
            category_path2, 
            path2_name, 
            original_price, 
            prod_sale_qty, 
            round(prod_sale_fixed_amt,4),
            row_number() OVER(partition BY t1.supplier_id,t2.supplier_name,t1.warehouse_name ORDER BY prod_sale_fixed_amt desc,prod_sale_qty desc) AS rn
        FROM ({}) t1
        LEFT JOIN ({}) t2
        ON t1.supplier_id = t2.supplier_num 
            AND t1.product_id = t2.product_id 
            AND t1.warehouse_id = t2.warehouse_id where t2.product_id is not null 
            and t2.product_id is not null and t2.warehouse_id is not null
    '''.format(data_date_begin,stock_table,order_send_detail_table)

    return hive_sql

def get_mayang_yunying_sql(date):

    end_date=date[0:7]+"-31"
    begin_date=date[0:7]+"-01"

    hive_sql='''
        SELECT
            '{}',
            supplier_code as supplier_num,
            supplier_name,
            prod_fixed_sale_amount as prod_sale_fixed_amt,
            round(prod_cost_purchase_return_amount / prod_cost_purchase_amount,4) as return_rate,
            round((prod_cost_stock_amount_start + prod_cost_stock_amount_end)/ 2 / prod_cost_sale_amount*currmonth_days,4) as zhouzhuan_days,
            round(prod_sold_amount / prod_skunums_stock_saleable_amount,4) as sku_rate,
            round(prod_gross_sale_amount / prod_net_sale_amount,4) as amao_rate,
            prod_skunums_stock_saleable_amount as qimo_sku_num,
            prod_cost_stock_saleable_amount as prod_stock_cost_amount,
            round(prod_skunums_stock_soldout_amount / prod_skunums_stock_saleable_amount,4) as duanhuo_rate
        from (
            select
                supplier_code,
                max(supplier_name) as supplier_name,
                max(prod_fixed_sale_amount) as prod_fixed_sale_amount,
                max(prod_cost_purchase_amount) as prod_cost_purchase_amount,
                max(prod_cost_purchase_return_amount) as prod_cost_purchase_return_amount,
                max(prod_cost_stock_amount_start) as prod_cost_stock_amount_start,
                max(prod_cost_stock_amount_end) as prod_cost_stock_amount_end,
                max(prod_cost_sale_amount) as prod_cost_sale_amount,
                datediff('{}','{}') as currmonth_days,
                max(prod_sold_amount) as prod_sold_amount,
                max(prod_skunums_stock_saleable_amount) as prod_skunums_stock_saleable_amount,
                max(prod_gross_sale_amount) as prod_gross_sale_amount,
                max(prod_net_sale_amount) as prod_net_sale_amount,
                max(prod_cost_stock_saleable_amount) as prod_cost_stock_saleable_amount,
                max(prod_skunums_stock_soldout_amount) as prod_skunums_stock_soldout_amount
            from 
                (
                    SELECT 
                        supplier_code,
                        supplier_name,
                        prod_fixed_sale_amount, 
                        null as prod_cost_purchase_amount, 
                        null as prod_cost_purchase_return_amount, 
                        null as prod_cost_stock_amount_start, 
                        null as prod_cost_stock_amount_end, 
                        prod_cost_sale_amount,
                        prod_skunums_sold_amount as prod_sold_amount, 
                        null as prod_skunums_stock_saleable_amount, 
                        prod_gross_sale_amount,
                        prod_net_sale_amount, 
                        null as prod_cost_stock_saleable_amount,
                        null as prod_skunums_stock_soldout_amount 
                    from (select 
                            supplier_num as supplier_code,
                            max(supplier_name) as supplier_name,
                            round(sum(case when sale_type = 1 then prod_sale_fixed_amt else 0 end), 4) as prod_fixed_sale_amount, 
                            round(sum(case when sale_type in(1,2) then gross_profit else 0 end), 4) as prod_gross_sale_amount, 
                            round(sum(case when sale_type in(1,2) then out_profit else 0 end), 4) as prod_net_sale_amount, 
                            round(sum(case when sale_type = 1 then cost_amount else 0 end), 4) as prod_cost_sale_amount, 
                            count(distinct case when sale_type = 1 and prod_sale_qty >= 1 then product_id else null end) as prod_skunums_sold_amount 
                            from dm_dws.dm_order_send_detail 
                            where data_date >= '{}' and data_date < '{}'
                            and supplier_num is not null and supplier_num != ''
                            group by 
                                supplier_num
                         )tmp1_send_res
			        union all
                    select 
                        supplier_code,
                        supplier_name,
                        null as prod_fixed_sale_amount,
                        null as prod_cost_purchase_amount, 
                        prod_cost_purchase_return_amount, 
                        null as prod_cost_stock_amount_start, 
                        null as prod_cost_stock_amount_end, 
                        null as prod_cost_sale_amount, 
                        null as prod_sold_amount, 
                        null as prod_skunums_stock_saleable_amount, 
                        null as prod_gross_sale_amount, 
                        null as prod_net_sale_amount, 
                        null as prod_cost_stock_saleable_amount, 
                        null as prod_skunums_stock_soldout_amount 
			from (
				select 
					suppliers.supplier_code,
					suppliers.supplier_name,
					res.prod_cost_purchase_return_amount
					from (
						select 
							supplier_key,
							sum(purchase_return_quantity *purchase_price) as prod_cost_purchase_return_amount 
						from dwd.prod_purchase_return_detail
						where trans_date >= '{}' and trans_date < '{}'
						group by supplier_key
						) res 
					left outer join dim.dim_supplier suppliers 
					on res.supplier_key = suppliers.supplier_key
				) tmp2_purchase_return_res
			union all
			select 
				supplier_code,
				supplier_name,
				null as prod_fixed_sale_amount, 
				prod_cost_purchase_amount, 
				null as prod_cost_purchase_return_amount, 
				null as prod_cost_stock_amount_start, 
				null as prod_cost_stock_amount_end, 
				null as prod_cost_sale_amount, 
				null as prod_sold_amount,
				null as prod_skunums_stock_saleable_amount, 
				null as prod_gross_sale_amount, 
				null as prod_net_sale_amount, 
				null as prod_cost_stock_saleable_amount, 
				null as prod_skunums_stock_soldout_amount 
			from (
					select 
						suppliers.supplier_code,
						suppliers.supplier_name,
						res.prod_cost_purchase_amount
						from (
						select 
							supplier_key,
							sum(purchase_quantity*purchase_price) as prod_cost_purchase_amount 
						from 
							dwd.prod_purchase_in_detail
						where trans_date >= '{}' and trans_date < '{}'
						group by supplier_key
						) res 
						left outer join dim.dim_supplier suppliers 
						on res.supplier_key = suppliers.supplier_key
				) tmp3_purchase_to_storage_res
			union all
			select 
				supplier_code,
				supplier_name,
				null as prod_fixed_sale_amount, 
				null as prod_cost_purchase_amount, 
				null as prod_cost_purchase_return_amount, 
				null as prod_cost_stock_amount_start, 
				prod_cost_stock_amount_end, 
				null as prod_cost_sale_amount, 
				null as prod_sold_amount, 
				prod_skunums_stock_saleable_amount,
				null as prod_gross_sale_amount, 
				null as prod_net_sale_amount, 
				prod_cost_stock_saleable_amount, 
				prod_skunums_stock_soldout_amount 
			from (
				select 
					suppliers.supplier_code,
					suppliers.supplier_name,
					res.prod_cost_stock_amount as prod_cost_stock_amount_end,
					res.prod_skunums_stock_saleable_amount,
					res.prod_cost_stock_saleable_amount,
					res.prod_skunums_stock_soldout_amount
					from (
						select 
							supplier_id,
							sum(prod_cost_stock_amount) as prod_cost_stock_amount,
						count(distinct case when prod_qty_stock_amount > 0 then product_id else null end) as prod_skunums_stock_saleable_amount,
						sum(prod_cost_stock_saleable_amount) as prod_cost_stock_saleable_amount,
						count(distinct case when prod_qty_stock_saleable_amount_tj <= 0 and prod_qty_stock_saleable_amount_gz <= 0 and prod_qty_stock_saleable_amount_wx <= 0 then product_id else null end) as prod_skunums_stock_soldout_amount 
						from (
							select 
								supplier_id,
								product_id,
								sum(physical_stock_quantity * purchase_price) as prod_cost_stock_amount,
								sum(physical_stock_quantity) as prod_qty_stock_amount,
								sum(
								case when (region_use_id in ('2', '3') or region_use_id in ('003', '004')) and ywbmid='4' then physical_stock_quantity*purchase_price
								when region_use_id is null and ywbmid in('2','3','5') then zpkcsy
								else 0 
								end
								) 
								as prod_cost_stock_saleable_amount,
								sum(
								case when warehouse_id =30 and (region_use_id in ('2', '3') or region_use_id in ('003', '004')) and ywbmid='4' then physical_stock_quantity
								when warehouse_id =30 and region_use_id is null and ywbmid in('2','3','5') then zpkc
								else 0 
								end
								) 
								as prod_qty_stock_saleable_amount_tj,
								sum(
								case when warehouse_id in(15,27) and (region_use_id in ('2', '3') or region_use_id in ('003', '004')) and ywbmid='4' then physical_stock_quantity
								when warehouse_id in(15,27) and region_use_id is null and ywbmid in('2','3','5') then zpkc
								else 0 
								end
								) 
								as prod_qty_stock_saleable_amount_gz,
								sum(
								case when warehouse_id =17 and (region_use_id in ('2', '3') or region_use_id in ('003', '004')) and ywbmid='4' then physical_stock_quantity
								when warehouse_id =17 and region_use_id is null and ywbmid in('2','3','5') then zpkc
								else 0 
								end
								) 
								as prod_qty_stock_saleable_amount_wx
								from dwd.prod_stock_supp_detail
								where trans_date = date_sub(cast(cast(concat(year('{}'),'-',month('{}'),'-01') as date) as string),1)
								group by supplier_id, product_id
						) tmp4_stock_res_qimo_pre
						group by supplier_id
					) res
					left outer join dim.dim_supplier suppliers 
					on res.supplier_id = suppliers.supplier_id
			) tmp4_stock_res_qimo
			union all
			select 
				supplier_code,
				supplier_name,
				null as prod_fixed_sale_amount, 
				null as prod_cost_purchase_amount, 
				null as prod_cost_purchase_return_amount, 
				prod_cost_stock_amount_start, 
				null as prod_cost_stock_amount_end, 
				null as prod_cost_sale_amount, 
				null as prod_sold_amount, 
				null as prod_skunums_stock_saleable_amount, 
				null as prod_gross_sale_amount, 
				null as prod_net_sale_amount, 
				null as prod_cost_stock_saleable_amount, 
				null as prod_skunums_stock_soldout_amount 
			from (
				select 
					suppliers.supplier_code,
					suppliers.supplier_name,
					res.prod_cost_stock_amount as prod_cost_stock_amount_start 
					from (
						select 
							supplier_id,
							sum(physical_stock_quantity * purchase_price) as prod_cost_stock_amount 
						from dwd.prod_stock_supp_detail
						where trans_date = '{}' 
						group by supplier_id
					) res 
					left outer join dim.dim_supplier suppliers 
					on res.supplier_id = suppliers.supplier_id
			)tmp5_stock_res_qichu
		) report_meta_res_union
	where
		supplier_code is not null
		and supplier_code != ''
	group by
		supplier_code 
	) report_meta_res
    '''.format(begin_date,end_date,begin_date,begin_date,end_date,begin_date,end_date,begin_date,end_date,end_date,end_date,begin_date)

    return hive_sql