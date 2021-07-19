


channel_base_sql = """ with  base as(
        select openid as distinct_id,permanentid,
        from_unixtime(cast(time/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as creation_time,url,
        (case when platform='wechat_mina' then '4' else platform end) as platform,lanuch_info,
        data_date from ods.miniprogramhour where data_date='{date}' and platform='wechat_mina' and type='1'
    ),
    
"""