from utils.date import get_tb_hb_date,get_month_end_day

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
    #月末最后一天
    data_date_end=get_month_end_day(data_date_begin)

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

def get_crm_mayang_yunying_sql(date):
    '''返回crm 系统sql'''
    #统计当月第一天
    begin_date = date[0:7] + "-01"
    #统计当月的下一个月的第一天
    month=str(int(date[5:7])+1)
    new_month=len(month)<2 and '0'+month or month
    end_date=date[0:4]+"-"+new_month+"-01"

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

#crm 系统
def get_crm_sql_data(datakey,tableinfokey,sqlcursor,table,date,month):
    '''
    从mysql中读数据
    :param datakey: 筛选条件
    :return:
    '''
    if month:
        sql_date=date[0:7]+"-01"
    else:
        sql_date = date[0:4] + "-01-01"
    supplier_num=set()
    product_id=set()
    warehouse_name=set()

    for ele in datakey:
        supplier_warehousename_product = ele.split('_')
        if len(supplier_warehousename_product)==1:
            supplier_num.add(supplier_warehousename_product[0])

        elif len(supplier_warehousename_product)==2:
            supplier_num.add(supplier_warehousename_product[0])
            product_id.add(int(supplier_warehousename_product[1]))

        else:
            supplier_num.add(supplier_warehousename_product[0])
            warehouse_name.add(supplier_warehousename_product[1])
            product_id.add(int(supplier_warehousename_product[2]))

    #数据表字段
    column=','.join([ele for ele in tableinfokey])
    supplier_in=','.join(["'"+str(ele)+"'" for ele in supplier_num])
    warehouse_in=','.join(["'"+str(ele)+"'" for ele in warehouse_name])
    product_in=','.join([str(ele) for ele in product_id])

    where=''
    groupby=' group by '
    if supplier_in !='':
        where += " supplier_num in (" + supplier_in +')'
        groupby += " supplier_num"
    if warehouse_in !='':
        where += " and warehouse_name in (" + warehouse_in +")"
        groupby += " ,warehouse_name"
    if product_in !='':
        where += " and product_id in (" + product_in +")"
        groupby += " ,product_id"

    mysql_sql=" select "+column+" from "+table+" where "+where+" and data_date='"+sql_date+"'"+groupby

    sqlcursor.execute(mysql_sql)
    sqldata=sqlcursor.fetchall()

    sql_data={}
    for each in sqldata:
        data = dict(zip(tableinfokey, each))
        supplier_num = data.pop('supplier_num')
        key=supplier_num

        if data.__contains__('warehouse_name'):
            warehouse_name = data.pop('warehouse_name')
            key += '_' + str(warehouse_name)

        if data.__contains__('product_id'):
            product_id = data.pop('product_id')
            key+= '_' + str(product_id)

        sql_data[key] = data
    return sql_data