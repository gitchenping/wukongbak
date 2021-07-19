import logging.config
from utils import util
from utils.db import connect_mongodb

logging.config.fileConfig("logging.conf")
viewlogger=logging.getLogger('view')

offline_sql='''
select 
		a.cust_id,
		a.product_id,
		a.from_platform,
		a.shop_id,
		a.last_supplier_id as supply_id,
		b.task_id
	from 
	(select
		a.cust_id,
		a.product_id,
		--a.push_ddclick,
		--a.creation_date,
		--nvl(a.shop_id,'-1') as shop_id,
		a.from_platform,
		(case when a.shop_id is null or a.shop_id=='null' or a.shop_id=='' then '-1' else a.shop_id end) as shop_id,
		nvl(b.last_supplier_id,'-1') as last_supplier_id
	from
		(
			select 
				a.cust_id,
				nvl(cast(nvl(a.product_id,0)as bigint),0) as product_id,
				(case when a.from_platform =='2' then '2002' 
				      when a.from_platform =='3' or a.from_platform =='7' then '2001' 
				      when a.from_platform =='0' then '1000' 
				      when a.from_platform =='12' then '3001' 
				      when a.from_platform =='21' then '2010'
				      when a.from_platform =='20' then '1200'
				      when a.from_platform=='26' then '2015'
				      when a.from_platform=='23' then '2011'
				      else -1
				 end) as from_platform ,
				concat(a.creation_date,' 00:00:00') as creation_date,
				a.shop_id
			from (
				select cust_id,data_date as creation_date,nvl(product_id,0) as product_id,app_id as from_platform,shop_id
				from 
					dwstage.clientbasicnew 
				where 
					data_date='2020-12-20' and (eventid in('7200') or (eventid='6000'))
				group by cust_id,nvl(product_id,0),category_path,app_id,case when eventid =='7200' then regexp_extract(click_content,'pid=(\\\d+)',1) end,
					substring(dateserver,1,10),data_date,shop_id 
				union all 
					select cust_id,data_date as creation_date,nvl(product_id,0) as product_id,
					case when ctr_type=='pc' then '0' when ctr_type=='touch' then '12' when ctr_type=='wechat' then '20' end as from_platform,
					shop_id 
					from 
						ddclick.webwapbasicnew 
					where 
						data_date='2020-12-20' and type = 1 
					group by cust_id,nvl(product_id,0) ,category_path,
						case when ctr_type=='pc' then '0' when ctr_type=='touch' then '12' when ctr_type=='wechat' then '20' end,
						substring(creation_time,1,10),data_date,shop_id 		
				union all 
					select b.cust_id,a.data_date as creation_date,nvl(product_id,0) as product_id,
					case when platform=='wechat_mina' then '21' when platform=='baidu_smart' then '23' when platform=='queickapp' then '26' end as from_platform,
					a.shop_id 
					from 
						ddclick.minibasicnew a
					left join
						(select cust_id,wx_open_id from dw_ods.customer_third_wechat_openid where wx_plat='2010') b 
							on a.openid=b.wx_open_id and b.wx_open_id<>''
					where 
							data_date='2020-12-20' and type = 1 
					group by b.cust_id,nvl(product_id,0) ,category_path,case when platform=='wechat_mina' then '21' when platform=='baidu_smart' then '23' 
							when platform=='queickapp' then '26' end ,substring(creation_time,1,10),data_date,shop_id 
			) a 
				where nvl(a.cust_id,'')<>'' and nvl(a.cust_id,'')<>'0' and a.cust_id>0				
		) a
		left join ddclick.item_book b on
				a.product_id = b.item_id) a
	inner join (
		select
			send_time,
			shop_id,
			supply_id,
			task_id,
			cust_id,
			product_id,
			reach_type
		from
			ods.ioc_auto_contact_pushid
		where
			trans_date = '2020-12-19' and send_time='2020-12-19' 
			and reach_type = 2 ) b 
	on
		a.cust_id = b.cust_id 
'''


def offline():
    cursor=util.connect_hive()
    cursor.execute(offline_sql)
    hive_data=cursor.fetchall()
    # hive_data=[[100000467,23655302,2002]]

    #
    conn_ck = util.connect_clickhouse()

    success=0
    fail=0
    for hivedata in hive_data:
        cust_id=hivedata[0]
        product_id=hivedata[1]
        from_platform=hivedata[2]
        if product_id==0:
            continue
        ck_sql="select toString(shop_id),supply_id,arrayElement(task_id,1) from ioc_mdata.page_view_detail_all_v2 where cust_id={} and product_id={} and from_platform='{}'".format(cust_id,product_id,from_platform)
        ck_sql=ck_sql+" and creation_date='2020-12-20 00:00:00'"
        conn_ck.execute(ck_sql)
        b =conn_ck.fetchall()
        diff_result = []
        if b is not None and len(b):
            ck_result=b[0]             #[(0, '2002', [107336669])]

            if ck_result[0] !=hivedata[3]:
                diff_result.append({'shop_id':{'hive':hivedata[3],'ck':ck_result[0]}})
            if ck_result[1] !=hivedata[4]:
                diff_result.append({'supply_id': {'hive': hivedata[4], 'ck': ck_result[1]}})
            if ck_result[2] != hivedata[5]:
                diff_result.append({'task_id': {'hive': hivedata[5], 'ck': ck_result[2]}})
        else:
            diff_result.append('ck不存在' )

        if len(diff_result)>0:
            #写日志
            viewlogger.info(str('cust_id='+str(hivedata[0])+" and "+"product_id="+str(hivedata[1])+" "+str(diff_result)))
            fail+=1
        else:
            success+=1
    print('success:' + str(success) + " fail:" + str(fail))


def realtime_mongo2ck():
    conn_ck = util.client_ck()

    #读mongo
    collection = util.connect_mongodb()

    start_date='2020-12-18 13:30:00'
    end_date='2020-12-18 13:59:59'
    a = collection.find({'creation_date': {"$gte":start_date,"$lte":end_date}},no_cursor_timeout=True)

    for item in a:
        cust_id=item['cust_id']
        product_id=item['product_id']
        from_platform=item['from_platform']
        creation_date=item['creation_date']

        ck_sql="select shop_id,supply_id,task_id from ioc_mdata.page_view_detail_all_v2 where cust_id={} and " \
               "product_id={} and from_platform='{}' and creation_date='{}'".format(cust_id,product_id,from_platform,creation_date)

        b = conn_ck.execute(ck_sql)

        diff_result = []
        if len(b):
            ck_result = b[0]  # [(0, '2002', [107336669])]
            mongo_shop_id=item['shop_id']
            mongo_supply_id=item['supply_id']
            mongo_task_id=item['task_id']

            if ck_result[0] != mongo_shop_id:
                diff_result.append({'shop_id': {'mongo': mongo_shop_id, 'ck': ck_result[0]}})
            if ck_result[1] != mongo_supply_id:
                diff_result.append({'supply_id': {'mongo': mongo_supply_id, 'ck': ck_result[1]}})
            if ck_result[2] != mongo_task_id:
                diff_result.append({'task_id': {'mongo': mongo_task_id, 'ck': ck_result[2]}})
        else:
            diff_result.append('ck不存在')

        if len(diff_result) > 0:
            # 写日志
            viewlogger.info(
                str('cust_id:' + str(cust_id) + " " + "product_id:" + str(product_id)+" "
                            ""+"from_platform:"+from_platform+ " " + "creation_date:" + creation_date+ str(diff_result)))
    a.close()
    pass


def realtime_mongo():

    start_date = '2020-12-18 12:00:00'
    end_date = '2020-12-18 13:59:59'
    # 读目的mongo
    collection= connect_mongodb()

    a = collection.find({'creation_date': {"$gte": start_date, "$lte": end_date}}, no_cursor_timeout=True)

    #supply_id
    b=connect_mongodb(database='interface',collection='item_book')
    #shop_id
    c=connect_mongodb(database='product_db_new',collection='products_core')

    #task_id
    redis_key='REACH_TASK_HAD_SENT_CUST_TASKID'
    r= util.get_redis()

    for item in a:
        cust_id = item['cust_id']
        product_id = item['product_id']
        shop_id = item['shop_id']                 #整型
        supply_id = item['supply_id']             #字符串
        task_id=item['task_id']                   #列表

        #读mongo 根据product_id获取supply_id
        supplyresult=list(b.find({'product_id':product_id}))
        supply_id_src=''
        if len(supplyresult)>0:
            supply_id_src=supplyresult[0]['supply_id']

        shopsult = list(c.find({'product_id': product_id}))
        shop_id_src=''
        if len(shopsult) > 0:
            shop_id_src = shopsult[0]['shop_id']

        #从redis读取task_id
        redis_filed=str(cust_id) + '#' + str(supply_id_src) + '#' + str(shop_id_src)
        task_info = r.hget(redis_key, redis_filed)
        print(task_info)
        if task_info is None:
            task_id_src=[]
        else:
            task_id_src=[eval(task_info)[0]['taskId']]

        diff_result=[]

        if shop_id != shop_id_src:
                diff_result.append({'shop_id': {'src mongo': shop_id_src, 'dst mongo': shop_id}})
        if supply_id!= supply_id_src:
                diff_result.append({'supply_id': {'src mongo': supply_id_src, 'dst mongo': supply_id}})
        if task_id!= task_id_src:
                diff_result.append({'task_id': {'src mongo': task_id_src, 'dst mongo': task_id}})


        if len(diff_result) > 0:
            # 写日志
            viewlogger.info(
                str('cust_id:' + str(cust_id) + " " + "product_id:" + str(product_id)  + str(diff_result)))
    a.close()
    pass