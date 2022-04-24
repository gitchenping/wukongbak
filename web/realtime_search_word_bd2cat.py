'''
clickhouse  bi_mdata.realtime_search_word_bd2cat_all  事业部与二级类的对应关系
'''
import os
import sys
import logging.config
from utils.db import connect_hive,client_ck
from utils.util import simplediff
from os import path

#logger
filepath=path.join(path.dirname(path.dirname(__file__)),"conf","logging.conf")
logging.config.fileConfig(filepath)
real_search_logger=logging.getLogger('jingying_overview')


sql='''select
	t0.bd_id,
	case
		when t0.bd_name in ('服装鞋包事业部') then '服装'
		when t0.bd_name in ('出版物事业部',
		'童书部') then '出版物'
		when t0.bd_name in ('数字业务事业部') then '数字'
		when t0.bd_name in ('文创-拍卖',
		'文创-文具',
		'文创-二手书') then '文创'
		when t0.bd_name in ('本地生活服务事业部',
		'日百事业部_孕婴童部',
		'海外购事业部',
		'新事业群—农村电商',
		'新事业群—农村电商',
		'日百事业部_食品美妆部',
		'数码3C事业部',
		'开放平台事业部',
		'日百事业部_家居家电部') then '百货'
		else null
	end as bd_name,
	-- t0.bd_name,
	t0.category_path,
	t1.path_name
from
	(
	select
		b.bd_id,
		b.bd_name,
		category_path
	from
		ods.bd_cat_map a
	inner join (
		select
			bd_id,
			bd_name
		from
			ods.bd
		where
			is_enabled = 1
		group by
			bd_id,
			bd_name )b on
		a.bd_id = b.bd_id
		and a.expiration_date = '2999-01-01'
		and split(a.category_path,'\\\\.')[2] = '00'
	group by
		b.bd_id,
		b.bd_name,
		category_path) t0
left join (
	select
		category_path,
		path_name
	from
		(
		select
			category_path,
			path_name,
			ROW_NUMBER() over(PARTITION by category_path
		order by
			last_changed_date desc ) as rank
		from
			ods.category a
		where
			split(a.category_path,'\\\\.')[2] = '00' ) t
	where
		rank = 1 ) t1 on
	t0.category_path = t1.category_path
'''

def do_job():
    hive_data = []
    column_keys =['bd_id','bd_name','category_path','path_name']

    databasename = 'ods'
    _,hive_cursor = connect_hive(database = databasename)

    hive_cursor.execute(sql)
    hive_data = hive_cursor.fetchall()

    ck_cursor = client_ck()

    ck_sql ="select * from iocTest1.realtime_search_word_bd2cat_all where category_path = '{}'"

    fail_num = 0
    total_num = len(hive_data)
    for h_data in hive_data:

        hive_dict = dict(zip(column_keys,h_data))

        ck_sql_format = ck_sql.format(hive_dict['category_path'])

        ck_data = ck_cursor.execute(ck_sql_format)
        ck_dict = {}
        if len(ck_data) >0:
            c_data = ck_data[0]
            ck_dict = dict(zip(column_keys,c_data))
        diffvalue = simplediff(hive_dict,ck_dict)
        if diffvalue != {}:
            fail_num += 1
            real_search_logger.info('filters:' +str(hive_dict))
            real_search_logger.info(diffvalue)
            real_search_logger.info('')

    success_num = total_num -fail_num
    print('run '+str(total_num)+" Fail:"+str(fail_num)+' Success:'+str(success_num))



if __name__ == '__main__':

    do_job()
