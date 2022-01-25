'''
app 拉新 sql
'''
sql = '''
with zhanwai_udid_source as(
select 
	udid,
	min(active_date) as active_time,
	1 as code,
	'抖音' as push_source
from ods.ocpc_common_data
where udid <>'' and source = 6 and status= 1
and to_date(active_date) = '2021-11-28'
group by udid
union all
select 
	device_id,
	min(action_time) as active_time,
	2 as code,
	'TD' as push_source
from ods.talkingdata
where device_id <>'' and data_date = '2021-11-28'
group by device_id
union all
select 
	udid,
	min(creation_date) as active_time,
	3 as code, 
	'ASA' as push_source
from ods.iphone_asa
where udid <>'' and trans_date = '2021-11-28'
group by udid
union all
select 
	distinct_id,
	concat('2021-11-28',' 00:00:00') as active_time,
	4 as code,
	brand as push_source
from dwd.channel_vivo_oppo_d_detail
where distinct_id <>'' and trans_date = '2021-11-28'
group by distinct_id,brand
),
zhanwai as(
select 
    udid,
    '' as from_platform,
    '' as medium,
    0 as new_flag,
    push_source,
    active_time,
    code,
    1 as data_type
from 
    zhanwai_udid_source
),
zhannei_app_flow_udid_platform as(
select 
    udid,
    from_platform,
    union_id,
    creation_date
from (
select 
    udid,
    case when app_id in ('iphone','ipad') then 'IOS' else 'Android' end as from_platform,
    union_id,
    creation_date,
    row_number() over(partition by udid order by creation_date) rank
from 
    ods.client_new
where 
    data_date = '2021-11-28'
    and union_id is not null and union_id <>''
    and udid is not null and udid <>''
    and app_id in ('iphone','ipad','android')
) t 
where rank =1
),
zhannei_app_flow_udid_new as(
select
	udid,
	case when trim(union_id)= '' then null else union_id end as union_id,
	data_date
from
	dwd.flow_app_udid_new
),
zhannei as (
select 
    a.udid,
    coalesce(c.from_platform,a.from_platform) as from_platform,
    coalesce(b.union_id,a.union_id) as union_id,
    case when b.data_date = '2021-11-28' then 1 else 0 end as new_flag,
    medium,
    a.creation_date,
    c.source,
    0 as code,
    0 as data_type
from 
    zhannei_app_flow_udid_platform a 
left join 
    zhannei_app_flow_udid_new b 
on a.udid = b.udid 
left join 
    tmp.channel_source_tmp c 
on coalesce(b.union_id,a.union_id) = c.channel_id 
),
udid_fromplatform_newflag_medium_source as(
select 
    udid,
    from_platform,
    new_flag,
    medium,
    source,
    data_type,
    lead(source,1) over(partition by udid order by code,creation_date) as push_source
from (
select 
    udid,
	from_platform,
	new_flag,
	medium,
	creation_date,
	source,
	data_type,
	code  
from zhannei 
union all 
select 
    udid,
	from_platform,
	new_flag,
	medium,
	active_time,
	push_source,
	data_type,
	code 
from zhanwai
) a 
),
main_key as (
select
    udid,
    from_platform,
    new_flag,
    coalesce(source,
    '其他') as source,
    coalesce(medium,
    '其他') as medium,
    coalesce(push_source,
    source,
    '其他') as push_source
from 
    udid_fromplatform_newflag_medium_source
where data_type = 0
),
order_create_detail as (
select
	device_id as udid,
	day_new_flag,
	sum(prod_sale_amt) as prod_sale_amt
from
	dm_dws.dm_order_create_detail
where
	data_date = '2021-11-28'
	and from_platform in (2,3,7)
	and sale_type = 1
group by
	device_id,
	day_new_flag ),
order_send_detail as (
select
	device_id as udid,
	day_new_flag,
	sum(prod_sale_amt) as prod_sale_amt,
	sum(out_profit) as out_profit
from
	dm_dws.dm_order_send_detail
where
	data_date = '2021-11-28'
	and from_platform in (2,3,7)
	and sale_type in (1,2)
group by
	device_id,
	day_new_flag )
select 
    a.udid,
	from_platform,
	source,
	medium,
	push_source,
	new_flag,
	case
		when b.day_new_flag is not null then 1
		else 0
	end as create_flag,
	case
		when c.day_new_flag is not null then 1
		else 0
	end as send_flag,
	nvl(b.day_new_flag,
	0) as create_new_flag,
	nvl(b.prod_sale_amt,
	0) as create_prod_sale_amt,
	nvl(c.day_new_flag,
	0) as send_new_flag,
	nvl(c.prod_sale_amt,
	0) as send_prod_sale_amt,
	nvl(c.out_profit,
	0) as send_out_profit,
	'2021-11-28' as data_date
from 
    main_key a 
left join 
    order_create_detail b 
on a.udid = b.udid 
left join order_send_detail c on
	a.udid = c.udid
'''