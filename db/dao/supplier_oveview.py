
#维度
weidu_dict = {
#1、一级渠道id
'source_id' :'''case when source='1' then 1 
     when  source in ('2','3','4')  then 2 
     else 3 
end as source_id''',
#2、一级渠道名称
'source_name' : '''case when source='1' then '主站' 
     when  source in ('2','3','4')  then '外卖场' 
     else '其他' 
end as source_name''',
#3、二级渠道id
'platform_id' : '''case when source=1 and from_platform='0' then 1 
     when source = 1 and from_platform in ('3','7','2') then 2
     when source = 1 and from_platform in ('21') then 3
     when from_platform in ('105') then 4
     when from_platform in ('101','102','103') then 5
     when from_platform in ('104') then 6
     else 7 
end as platform_id''',
#4、二级渠道名称
'platform_name' : '''case when source=1 and from_platform='0' then 'PC' 
     when  source=1 and from_platform in('3','7','2') then 'app' 
     when  source=1 and from_platform in ('21') then '小程序' 
     when from_platform in ('105') then '拼多多'
     when from_platform in ('101','102','103') then '天猫'
     when from_platform in ('104') then '抖音'
     else '其他'
end as platform_name'''
}

def get_test_sql(date_begin,date_end):
    '''

    :param date_str:
    :return:
    '''

    pay_sql = '''with pay_base as(
        select 
        supplier_num,
        {source_id},
        {source_name},
        {platform_id},
        {platform_name},
        prod_sale_fixed_amt,
        prod_sale_amt,
        cust_id,
        parent_id,
        prod_sale_qty 
        from dm_dws.dm_order_pay_detail 
        where data_date >='{date_begin}' and data_date <= '{date_end}' 
        and bd_id !=6 and sale_type=1 and supplier_num is not null and trim(supplier_num) != ''
    ),
    '''.format(**weidu_dict,date_begin = date_begin,date_end= date_end)
    cancel_sql = '''cancel_base as(
        select 
        supplier_num,
        {source_id},
        {source_name},
        {platform_id},
        {platform_name},
        parent_id
        from dm_dws.dm_order_cancel_detail 
        where data_date >='{date_begin}' and data_date <= '{date_end}' 
        and bd_id !=6 and supplier_num is not null and trim(supplier_num) != ''
    ),
    '''.format(**weidu_dict,date_begin = date_begin,date_end= date_end)

    t = '''union_table as(select
   supplier_num,
   source_id,
   source_name,
   platform_id,
   platform_name,
   sum(prod_sale_fixed_amt) prod_sale_fixed_amt,
   sum(prod_sale_amt) prod_sale_amt,
   sum(pay_cust_num) pay_cust_num,
   sum(pay_order_num) pay_order_num,
   sum(pay_product_num) pay_product_num,
   sum(cancel_order_num) cancel_order_num
   from (
        select 
          supplier_num,
          source_id,
          source_name,
          platform_id,
          platform_name,
          sum(prod_sale_fixed_amt) prod_sale_fixed_amt,
          sum(prod_sale_amt) prod_sale_amt,
          count(distinct cust_id) pay_cust_num,
          count(distinct parent_id) pay_order_num,
          sum(prod_sale_qty) pay_product_num,
          0 as cancel_order_num
          from
          pay_base
          group by 
          supplier_num,
          source_id,
          source_name,
          platform_id,
          platform_name
          union all 
          select 
          supplier_num,
          source_id,
          source_name,
          platform_id,
          platform_name,
          0 prod_sale_fixed_amt,
          0 prod_sale_amt,
          0 pay_cust_num,
          0 pay_order_num,
          0 pay_product_num,
          count(distinct parent_id) cancel_order_num
          from cancel_base
          group by 
          supplier_num,
          source_id,
          source_name,
          platform_id,
          platform_name
          ) t 
   group by 
   supplier_num,
   source_id,
   source_name,
   platform_id,
   platform_name
    ) 
    '''
    _sql = '''select 
a.supplier_num,                                                 --供应商编码
source_id,                                                      --一级渠道id
source_name,                                                    --一级渠道名称
platform_id,                                                    --二级渠道id
platform_name,                                                  --二级渠道名称
round(prod_sale_fixed_amt,4),                                   --支付码洋
round(a.prod_sale_amt,4),                                       --支付实洋
pay_cust_num,                                                   --支付用户数
pay_order_num,                                                  --支付订单数
pay_product_num,                                                --支付商品件数
cancel_order_num,                                               --取消订单数
round(a.prod_sale_amt/b.prod_sale_amt,4) prod_sale_amt_rate,    --支付实洋占比(每个渠道支付实洋占供应商总支付实洋的比例)
round(a.prod_sale_amt/pay_cust_num,4) pay_atv,                  --支付客单价（支付实洋/支付用户数）
round(cancel_order_num/pay_order_num,4) cancel_order_rate,
'{date_end}' as data_date
from union_table a
left join (
select 
supplier_num,
sum(prod_sale_amt) prod_sale_amt
from
pay_base
group by 
supplier_num
)b 
on a.supplier_num = b.supplier_num'''.format(date_end = date_end)

    sql = pay_sql+cancel_sql+t+_sql
    return sql