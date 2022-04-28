'''
搜索词分析 sql
'''
from utils import db

import pandas as pd

#sql
ck_sql ='''select 
t0.data_date as date_str,
'{platform_name}' as platform,
'{searchtype_name}' as searchtype,
t0.search_word,
t1.search_times,
t1.search_uv,
t2.click_times,
t2.click_uv,
case when t1.search_uv <>0 then round(t2.click_uv / t1.search_uv,2) else null end as click_search_ratio,
t3.create_cust_num,
case when t1.search_uv <>0 then round(t3.create_cust_num / t1.search_uv,2) else null end as create_cust_ratio,
t3.create_parent_num,
t3.create_sale_amt,
t3.create_quantity_num,
t4.no_search_times,
case when t1.search_times <>0 then round(t4.no_search_times / t1.search_times,2) else null end as no_search_times_ratio,
t5.search_no_click,
t6.median_sku_max_ex_location,
t7.median_sku_max_cl_location,
t6.avg_sku_max_ex_location,
t7.avg_sku_max_cl_location,
case when t1.search_pv <>0 then round(t5.search_no_click / t1.search_pv,2) else null end as jump_over_ratio,
case when t1.search_times <>0 then round(t3.create_sale_amt / t1.search_times,2)*1000 else null end as rpm,
case when t1.search_uv <>0 then round(t3.create_sale_amt / t1.search_uv,2) else null end as search_uv_value,
t8.next_search_word_time,
t3.zf_prod_sale_amt,
t3.out_pay_amount
from 
( select 
search_word,
data_date
from default.search_order_detail_2
where event_id IN (4311, 4201, 4031, 6403) AND LENGTH(search_word) > 0 and {where} 
group by data_date,search_word) t0 
left join 
(select 
search_word, 
COUNT(1) AS search_pv, 
COUNT(DISTINCT udid) AS search_uv, 
SUM(CASE WHEN last_cseq == root_cseq THEN 1 ELSE 0 END) AS search_times,
data_date
from default.search_order_detail_2 
where event_id = 4311 and {where} 
group by search_word,data_date
) t1
on t0.search_word = t1.search_word and t0.data_date = t1.data_date
left join(
SELECT
	t1.data_date,
	t1.search_word AS search_word, 
	COUNT(DISTINCT t1.udid, t1.creation_date, t1.main_product_id) AS click_times, 
	COUNT(DISTINCT t1.udid) AS click_uv
	FROM
	    (
	    SELECT
            search_word, udid, last_cseq,data_date,creation_date,main_product_id
        FROM
            default.search_order_detail_2
        WHERE
            event_id IN (4201, 4031)) t1
	LEFT JOIN
	    (
	    SELECT
            last_cseq,data_date,search_word
        FROM
            default.search_order_detail_2
        WHERE
            event_id = 4311 and {where}) t2 
    ON t1.last_cseq = t2.last_cseq and t1.data_date = t2.data_date and t1.search_word =t2.search_word
    WHERE
    	t2.last_cseq <> '' 
    GROUP BY t1.search_word,t1.data_date
)t2
on t0.search_word = t2.search_word and t0.data_date = t2.data_date
left join 
(SELECT
    t1.data_date,
	t1.search_word AS search_word,
	COUNT(DISTINCT t1.udid) AS create_cust_num, 
	count(DISTINCT t1.order_id) as create_parent_num,
	sum(order_quantity) as create_quantity_num,
	round(SUM(t1.bargin_price * t1.order_quantity),2) AS create_sale_amt,
	round(sum(t1.zf_prod_sale_amt),2) as zf_prod_sale_amt,
	round(sum(t1.out_pay_amount),2) as out_pay_amount
    FROM
        (
        SELECT
            data_date,search_word, order_id,udid,zf_prod_sale_amt,out_pay_amount, 
            bargin_price, order_quantity, last_cseq
        FROM
            default.search_order_detail_2
        WHERE
            order_id >= 0 AND LENGTH(last_cseq) > 0) t1
    LEFT JOIN
        (
        SELECT
            last_cseq,data_date,search_word
        FROM
            default.search_order_detail_2
        WHERE
            event_id = 4311 and {where}) t2
    ON t1.last_cseq = t2.last_cseq and t1.data_date = t2.data_date and t1.search_word =t2.search_word
    WHERE t2.last_cseq <> ''
    GROUP BY
    	t1.search_word,t1.data_date
)t3
on t0.search_word = t3.search_word and t0.data_date = t3.data_date
left join(
SELECT
	data_date, search_word, 
	COUNT(1) AS no_search_times, 
	COUNT(DISTINCT udid) AS no_search_uv
	FROM
		default.search_order_detail_2
	WHERE
		event_id = 4311 AND search_num = 0 and {where}
	group by data_date,search_word
)t4 
on t0.search_word = t4.search_word and t0.data_date = t4.data_date
left join(
SELECT
	t1.search_word,
	t1.data_date,
	COUNT(1) AS search_no_click
	FROM
		(
		SELECT
			data_date,search_word, last_cseq
		FROM
			default.search_order_detail_2
		WHERE
			event_id = 4311 AND LENGTH(last_cseq) > 0 and {where}) t1
	LEFT JOIN (
		SELECT
			last_cseq,data_date,search_word
		FROM
			default.search_order_detail_2
		WHERE
			event_id IN (4201, 4031)) t2
	ON t1.last_cseq = t2.last_cseq and t1.data_date = t2.data_date and t1.search_word =t2.search_word
	WHERE
		t2.last_cseq = ''
	GROUP BY
		t1.search_word,t1.data_date
) t5
on t0.search_word = t5.search_word and t0.data_date = t5.data_date
left join(
SELECT
	data_date,
	search_word,
	ROUND(SUM(t.max_position)/ SUM(t.one)) + 1 AS avg_sku_max_ex_location,
    medianExact(0.5)(t.max_position) + 1 AS median_sku_max_ex_location
	FROM
	(
        SELECT
           t1.data_date, t1.search_word, 1 AS one, MAX(t1.position) AS max_position
        FROM
            (
            SELECT
                data_date, search_word, position, last_cseq
            FROM
                default.search_order_detail_2
            WHERE
                event_id = 6403 AND position >= 0 AND LENGTH(last_cseq) > 0) t1
        LEFT JOIN
            (
            SELECT
                last_cseq,data_date,search_word
            FROM
                default.search_order_detail_2
            WHERE
                event_id = 4311
                and {where}
                ) t2
        ON t1.last_cseq = t2.last_cseq and t1.data_date = t2.data_date and t1.search_word =t2.search_word
        WHERE
            t2.last_cseq <> ''
        GROUP BY
             t1.search_word, t1.data_date,t1.last_cseq
        ) t
    GROUP BY
		data_date,search_word
)t6 
on t0.search_word = t6.search_word and t0.data_date = t6.data_date
left join(
SELECT
	    data_date,
        search_word,
	    ROUND(SUM(t.max_position)/ SUM(t.one)) + 1 AS avg_sku_max_cl_location,
        medianExact(0.5)(t.max_position) + 1 AS median_sku_max_cl_location
	FROM
	    (
        SELECT
            t1.data_date,t1.search_word, 1 AS one, MAX(t1.position) AS max_position
        FROM
            (
            SELECT
                data_date, search_word,search_word, position,last_cseq
            FROM
                default.search_order_detail_2
            WHERE
                event_id IN (4201, 4031) AND position >= 0 AND LENGTH(last_cseq) > 0) t1
        LEFT JOIN
            (
            SELECT
                last_cseq,data_date,search_word
            FROM
                default.search_order_detail_2
            WHERE
                event_id = 4311 and {where}) t2
        ON t1.last_cseq = t2.last_cseq and t1.data_date = t2.data_date and t1.search_word =t2.search_word
        WHERE
            t2.last_cseq <> ''
        GROUP BY
            t1.data_date,t1.search_word, t1.last_cseq) t
    GROUP BY
	    data_date, search_word
) t7
on t0.search_word = t7.search_word and t0.data_date = t7.data_date
left join(
SELECT
    date_str,
    current_search_word AS search_word,
    SUM(be_searched_time) AS next_search_word_time
    FROM default.dm_sr_kpi_next_search_word_2
WHERE 
   {next_word_where}
GROUP by current_search_word,date_str    
)t8
on t0.search_word = t8.search_word and t0.data_date = t8.date_str
'''


def get_sql(data):

    #event_id IN (4311, 4201, 4031, 6403) AND LENGTH(search_word) > 0
    start_time = data['start']
    end_time = data['end']

    where =" data_date between '"+start_time+"' and '"+end_time+"' "


    if data.__contains__('search_word'):
        search_word = data['search_word']
        if search_word != '':
            where +=" and search_word = "+search_word

    next_where = where.replace('data_date', 'date_str')

    platformname = '全部'
    if data.__contains__('platformname'):
        platformname = data['platformname']
        app_id = get_platform_id(platformname)
        if platformname != "":
            where += " and app_id = '"+str(app_id)+"'"

            next_where = next_where+" and platform = '"+str(app_id)+"' "
    else:
        next_where += " and platform = '1' "


    searchtypename = '全部'
    if data.__contains__('searchtype'):
        searchtypename = data['searchtype']
        search_type = get_search_type(searchtypename)
        if search_type != '':
            where += " and search_type = "+search_type
    else:
        where += " and search_type in (1,2,3,4,5,6) "

    where += " and length(search_word) >0 "

    sql = ck_sql.format(where=where,next_word_where= next_where,platform_name = platformname,searchtype_name = searchtypename)
    return sql



def get_sql_data(data):
    '''

    :param data: 筛选入参
    :return:
    '''
    ck_db = db.PyCK()
    filters = data ['filters']
    params = data['params']

    searchword = ''
    if filters is not None:
        searchword = filters[0]['value']

    params_dict = {ele['name']:ele['value'].strip("'") for ele in params}
    params_dict.update({'search_word':searchword})

    sql = get_sql(params_dict)
    sql_data_list = ck_db.get_result_from_db(sql)


    #表头
    table_column = ['date_str','平台','搜索来源','search_word']+["sum("+ele['column']+")" for ele in data['aggregators']]

    df = pd.DataFrame(sql_data_list,columns = table_column)

    return df
