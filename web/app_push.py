'''
渠道分析-App拉新分析
结果表:
    ck:dm_report.app_push_source_analysis_detail
'''

app_push_source_analysis_detail_table_en_cn ={
 'udid' :  '设备id',
 'from_platform' :  '客户端ios,android',
 'source' :  '下载渠道',
 'medium' :  '投放类型',
 'push_source' :  '投放渠道',
 'new_flag' :  '当天是否新udid标识(激活)1,0',
 'create_flag' :  '当天是否收订标识1,0',
 'send_flag' :  '当天是否出库订单标识1,0',
 'create_new_flag' :  '当天是否是收订新客标识1,0',
 'create_prod_sale_amt' :  '收订金额',
 'send_new_flag' :  '当天是否出库新客标识1,0',
 'send_prod_sale_amt' :  '出库金额',
 'send_out_profit' :  '出库净销售额'
}

indicator_cal_map ={
    #当日
    'new_uv' : 'count(case when new_flag=1 then 1 else null end) as new_uv',
    #收订新客数
    'create_new_num':'count(case when new_flag=1 and create_new_flag=1 then 1 else null end) as create_new_num',
    #收订顾客数
    'create_num_day':'count(case when new_flag=1 and create_flag=1 then udid else null end) as create_num_day',
    #新客收订金额
    'create_new_prod_sale_amt':'sum(case when new_flag=1 then create_prod_sale_amt else 0 end) as create_new_prod_sale_amt',
    #出库新客数
    'send_new_num':'count(case when new_flag=1 and send_new_flag=1 then 1 else null end) as send_new_num',
    #出库顾客数
    'send_num_day':'count(case when new_flag=1 and send_flag=1 then udid else null end) as send_num_day',
    #出库金额
    'send_new_prod_sale_amt':'sum(case when new_flag=1 then send_prod_sale_amt else 0 end) as send_new_prod_sale_amt',
    #
    'send_new_out_profit':'sum(case when new_flag=1 then send_out_profit else 0 end) as send_new_out_profit',


}
