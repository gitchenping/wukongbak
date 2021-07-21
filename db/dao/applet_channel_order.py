from utils.date import get_tb_hb_date,get_month_end_day


wechat_order_detail_sql ='''
    with create_detail as (
        select
             t.platform,
             t.cust_id,
             t.parent_id,
             t.order_id,
             t.item_id,
             t.out_profit,
             t.out_pay_amount,
             t.day_new_flag,
             (case when t1.cust_id is not null then '1' else '2' end) as week_new_flag,
             t.month_new_flag,
             (case when t2.cust_id is not null then '1' else '2' end) as quarter_new_flag,
             t.sale_type,
             t.bd_id,
             t.data_date
        from
             (
                 select
                     platform,
                     cust_id,
                     parent_id,
                     order_id,
                     item_id,
                     out_profit,
                     out_pay_amount,
                     day_new_flag,
                     month_new_flag,
                     sale_type,
                     bd_id,
                     data_date
                 from dm_dws.dm_order_create_detail
                 where data_date='{date}' and platform='4'
             ) t
             left join(
                select cust_id from (
                    select
                        cust_id
                    from dwd.cust_create_new_detail
                    where trans_date>='{week_monday}' and trans_date<='{date}'
                    group by cust_id
                    ) create_week) t1 on (t.cust_id=t1.cust_id)
             left join(
                select cust_id from (
                    select
                        cust_id
                    from dwd.cust_create_new_detail
                    where trans_date>='{quarter_day}' and trans_date<='{date}'
                    group by cust_id
                ) create_quarter) t2 on (t.cust_id=t2.cust_id)
        ),
        
    pay_detail as (
        select
             t.platform,
             t.cust_id,
             t.parent_id,
             t.order_id,
             t.item_id,
             t.out_profit,
             t.out_pay_amount,
             t.day_new_flag,
             (case when t1.cust_id is not null then '1' else '2' end) as week_new_flag,
             t.month_new_flag,
             (case when t2.cust_id is not null then '1' else '2' end) as quarter_new_flag,
             t.sale_type,
             t.bd_id,
             t.data_date
        from
             (
                 select
                     platform,
                     cust_id,
                     parent_id,
                     order_id,
                     item_id,
                     out_profit,
                     out_pay_amount,
                     day_new_flag,
                     month_new_flag,
                     sale_type,
                     bd_id,
                     data_date
                 from dm_dws.dm_order_pay_detail
                 where data_date='{date}' and platform='4'
             ) t
             left join(
                select cust_id from (
                    select
                        cust_id
                    from dwd.cust_pay_new_detail
                    where trans_date>='{week_monday}' and trans_date<='{date}'
                    group by cust_id
                    ) create_week) t1 on (t.cust_id=t1.cust_id)
             left join(
                select cust_id from (
                    select
                        cust_id
                    from dwd.cust_pay_new_detail
                    where trans_date>='{quarter_day}' and trans_date<='{date}'
                    group by cust_id
                ) create_quarter) t2 on (t.cust_id=t2.cust_id)
        ),
    
    send_detail as (
        select
             t.platform,
             t.cust_id,
             t.parent_id,
             t.order_id,
             t.item_id,
             t.out_profit,
             t.out_pay_amount,
             t.day_new_flag,
             (case when t1.cust_id is not null then '1' else '2' end) as week_new_flag,
             t.month_new_flag,
             (case when t2.cust_id is not null then '1' else '2' end) as quarter_new_flag,
             t.sale_type,
             t.bd_id,
             t.data_date
        from
             (
                 select
                     platform,
                     cust_id,
                     parent_id,
                     order_id,
                     item_id,
                     out_profit,
                     out_pay_amount,
                     day_new_flag,
                     month_new_flag,
                     sale_type,
                     bd_id,
                     data_date
                 from dm_dws.dm_order_send_detail
                 where data_date='{date}' and platform='4'
             ) t
             left join(
                select cust_id from (
                    select
                        cust_id
                    from dwd.cust_send_new_detail
                    where trans_date>='{week_monday}' and trans_date<='{date}'
                    group by cust_id
                    ) create_week) t1 on (t.cust_id=t1.cust_id)
             left join(
                select cust_id from (
                    select
                        cust_id
                    from dwd.cust_send_new_detail
                    where trans_date>='{quarter_day}' and trans_date<='{date}'
                    group by cust_id
                ) create_quarter) t2 on (t.cust_id=t2.cust_id)
        ),
        
    create_channel_detail as(
        select
            (case when t1.distinct_id is null then 'null' else t1.distinct_id end) as distinct_id,
            (case when t1.creation_time is null then 'null' else t1.creation_time end) as creation_time,
            (case when t1.union_id is null then 'null' else t1.union_id end) as union_id,
            (case when t1.channel1 is null then '自然量' else t1.channel1 end) as channel1,
            (case when t1.channel2 is null then 'null' else t1.channel2 end) as channel2,
            (case when t1.channel3 is null then 'null' else t1.channel3 end) as channel3,
            (case when t1.channel4 is null then 'null' else t1.channel4 end) as channel4,
            (case when t1.channel5 is null then 'null' else t1.channel5 end) as channel5,
            t.cust_id,
            t.parent_id,
            t.order_id,
            t.item_id,
            t.out_profit,
            t.out_pay_amount,
            t.day_new_flag,
            t.week_new_flag,
            t.month_new_flag,
            t.quarter_new_flag,
            t.platform,
            '1' as order_status,
            t.sale_type,
            t.bd_id,
            t.data_date
        from
			create_detail t
        left join(
            select
                permanent_id as distinct_id,
                creation_time,
                union_id,
                channel1,
                channel2,
                channel3,
                channel4,
                channel5,
                item_id
            from dwd.channel_mini_wechat_order_attri_detail
            where data_date='{date}'
            ) t1
        on (t.item_id=t1.item_id)
    ),
    
    pay_channel_detail as(
        select
            (case when t1.distinct_id is null then 'null' else t1.distinct_id end) as distinct_id,
            (case when t1.creation_time is null then 'null' else t1.creation_time end) as creation_time,
            (case when t1.union_id is null then 'null' else t1.union_id end) as union_id,
            (case when t1.channel1 is null then '自然量' else t1.channel1 end) as channel1,
            (case when t1.channel2 is null then 'null' else t1.channel2 end) as channel2,
            (case when t1.channel3 is null then 'null' else t1.channel3 end) as channel3,
            (case when t1.channel4 is null then 'null' else t1.channel4 end) as channel4,
            (case when t1.channel5 is null then 'null' else t1.channel5 end) as channel5,
            t.cust_id,
            t.parent_id,
            t.order_id,
            t.item_id,
            t.out_profit,
            t.out_pay_amount,
            t.day_new_flag,
            t.week_new_flag,
            t.month_new_flag,
            t.quarter_new_flag,
            t.platform,
            '2' as order_status,
            t.sale_type,
            t.bd_id,
            t.data_date
        from
			pay_detail t
        left join(
            select
                permanent_id as distinct_id,
                creation_time,
                union_id,
                channel1,
                channel2,
                channel3,
                channel4,
                channel5,
                item_id
            from dwd.channel_mini_wechat_order_attri_detail
            where where data_date>='{last2month}' and data_date<='{date}'
            ) t1
        on (t.item_id=t1.item_id)
    
    ),
    
    send_channel_detail as(
        select
            (case when t1.distinct_id is null then 'null' else t1.distinct_id end) as distinct_id,
            (case when t1.creation_time is null then 'null' else t1.creation_time end) as creation_time,
            (case when t1.union_id is null then 'null' else t1.union_id end) as union_id,
            (case when t1.channel1 is null then '自然量' else t1.channel1 end) as channel1,
            (case when t1.channel2 is null then 'null' else t1.channel2 end) as channel2,
            (case when t1.channel3 is null then 'null' else t1.channel3 end) as channel3,
            (case when t1.channel4 is null then 'null' else t1.channel4 end) as channel4,
            (case when t1.channel5 is null then 'null' else t1.channel5 end) as channel5,
            t.cust_id,
            t.parent_id,
            t.order_id,
            t.item_id,
            t.out_profit,
            t.out_pay_amount,
            t.day_new_flag,
            t.week_new_flag,
            t.month_new_flag,
            t.quarter_new_flag,
            t.platform,
            '2' as order_status,
            t.sale_type,
            t.bd_id,
            t.data_date
        from
			(
			    select
                     platform,
                     cust_id,
                     parent_id,
                     order_id,
                     sum(out_profit) as out_profit,
                     sum(out_pay_amount) as out_pay_amount,
                     day_new_flag,
                     week_new_flag,
                     month_new_flag,
                     quarter_new_flag,
                     sale_type,
                     bd_id,
                     data_date
                from 
                        send_detail
                group by
                     platform,
                     cust_id,
                     parent_id,
                     order_id,
                     day_new_flag,
                     week_new_flag,
                     month_new_flag,
                     quarter_new_flag,
                     sale_type,
                     bd_id,
                     data_date 
			) t
        left join(
            select
                permanent_id as distinct_id,
                creation_time,
                union_id,
                channel1,
                channel2,
                channel3,
                channel4,
                channel5,
                item_id
            from dwd.channel_mini_wechat_order_attri_detail
            where where data_date>='{lastyear}' and data_date<='{date}'
            ) t1
        on (t.order_id=t1.order_id)
    )
    
    select
         t.distinct_id,
         t.creation_time,
         t.union_id,
         t.channel1,
         t.channel2,
         t.channel3,
         t.channel4,
         t.channel5,
         cast(t.cust_id as string) as cust_id,
         cast(t.parent_id as string) as parent_id,
         cast(t.order_id as string) as order_id,
         cast(t.item_id as string) as item_id,
         cast(round((case when t.out_profit is null then 0.00 else t.out_profit end),2) as string) as out_profit,
         cast(round((case when t.out_pay_amount is null then 0.00 else t.out_pay_amount end),2) as string) as out_pay_amount,
         cast(t.day_new_flag as string) as day_new_flag,
         t.week_new_flag,
         cast(t.month_new_flag as string) as month_new_flag,
         t.quarter_new_flag,
         cast(t.platform as string) as platform,
         t.order_status,
         cast(t.sale_type as string) as sale_type,
         cast(t.bd_id as string) as bd_id,
         t.data_date
    from
         (
         select
         distinct_id,creation_time,union_id,channel1,channel2,channel3,channel4,channel5,cust_id,parent_id,order_id,item_id,out_profit,out_pay_amount,day_new_flag,week_new_flag,month_new_flag,quarter_new_flag,platform,order_status,sale_type,bd_id,data_date
         from create_channel_detail
         union all
         select
         distinct_id,creation_time,union_id,channel1,channel2,channel3,channel4,channel5,cust_id,parent_id,order_id,item_id,out_profit,out_pay_amount,day_new_flag,week_new_flag,month_new_flag,quarter_new_flag,platform,order_status,sale_type,bd_id,data_date
         from pay_channel_detail
         union all
         select
         distinct_id,creation_time,union_id,channel1,channel2,channel3,channel4,channel5,cust_id,parent_id,order_id,item_id,out_profit,out_pay_amount,day_new_flag,week_new_flag,month_new_flag,quarter_new_flag,platform,order_status,sale_type,bd_id,data_date
         from send_channel_detail
         ) t
'''