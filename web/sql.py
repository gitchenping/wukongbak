from utils import util
from utils._sql import get_uv_sql_for_report,get_sd_info_sql_for_report,get_zf_info_sql_for_report
from  utils._sql import get_drill_tb_hb

app_id={'全部':"('2','3')",'IOS':"('2')",'安卓':"('3')"}
product_is_merchant={'自营':'(1)','招商':'(2)','全部':'(1,2)'}

bd_id={'出版物':'(1)','日百服':'(2)','数字':'(3)','文创':'(4)','其他':'(5)','全部':'(1,2,4,5)'}


conn_ck = util.connect_clickhouse(host='10.7.30.148')

def get_filters_where_for_reco(filters):
    '''推荐-单品分析筛选条件'''

    wheredict=dict(filters)
    where=''
    #日期
    data_date=wheredict['start']
    where += "data_date='" + data_date + "'"+" and "
    #平台
    appid=" in "+app_id[wheredict['platform_name']]
    where+="app_id "+ appid+ " and "
    #经营方式
    shoptype=" in "+product_is_merchant[wheredict['shop_type_name']]
    where+="product_is_merchant"+shoptype+ " and "
    #事业部
    bd =" in " +bd_id[wheredict['bd_name']]
    where+="bd_id "+bd+" and "
    #二级品类

    path2_name=wheredict['path2_name']
    if path2_name!='全部':
        where+="path2_name='"+path2_name+"'"+" and "

    #页面
    page_name=wheredict['page_name']

    if page_name!='全部':
        where+="model_page_name='"+page_name+"'"+" and "

    #模块
    module_name=wheredict['model_cn_name']
    if module_name != '全部':
        where+="model_cn_name='"+module_name+"'"

    return where
    pass

def get_sql_data_reco(data,indicator_cal_map,filters):
    # 获取筛选条件
    where = get_filters_where_for_reco(filters)

    date=data['日期']
    product_id = data['商品ID']
    product_name = data['商品名称']
    where += ' and product_id= ' + str(product_id) + " and product_name= '" + str(product_name) + "'"

    # sql
    column_all = ''
    for _, column in indicator_cal_map.items():
        column_all += column + ","
    final_column = column_all.strip(',')

    sql = "select " + final_column + " from dm_report.reco_order_detail " + " where " + where

    conn_ck.execute(sql)
    ck_data = conn_ck.fetchall()

    sqldata={}
    if len(ck_data)>0:
        sqldata=dict(zip(indicator_cal_map.keys(),ck_data[0]))
        sqldata['日期']=date
        sqldata['商品ID']=product_id
        sqldata['商品名称']=product_name

    return sqldata

def report_sql_uv(data):
    # uv table
    uv_table = 'bi_mdata.mdata_flows_user_realtime_all'
    datacopy=dict(data)

    sql=get_uv_sql_for_report(datacopy,uv_table)
    conn_ck.execute(sql)
    ck_data = conn_ck.fetchall()
    sqldata = {}
    if len(ck_data) > 0:
        namedict={ ele[0]:ele[0] for ele in ck_data}
        uv=get_drill_tb_hb(ck_data, namedict, data['queryDate'], '',misskeyshow=True,misskeyvalue='-')

        for key, value in uv.items():
            atemp = {}
            for ikey, ivalue in value.items():
                if ikey == 'value':
                    newkey = 'uv'
                elif ikey == '同比去年':
                    newkey = 'uvYoY'
                else:
                    continue
                atemp[newkey] = ivalue
            sqldata[key]=atemp
    return sqldata

def report_sql_sdzf_info(data):
    # sd zf table
    sd_zf_indicator_table = 'bi_mdata.kpi_order_info_all_v2'
    sd_zhibiao_list=['subsAmount','subsPackages','subsCustomer','subsNewCustomer','subsCxlRate']
    zf_zhibiao_list=['pymtAmount','pymtPackages','pymtCustomer','pymtNewCustomer','pymtCxlRate']
    datacopy = dict(data)

    #收订
    sql = get_sd_info_sql_for_report(datacopy, sd_zf_indicator_table)

    conn_ck.execute(sql)
    ck_data = conn_ck.fetchall()
    sd_sqldata = {}
    if len(ck_data) > 0:
        namedict = {ele[0]: ele[0] for ele in ck_data}

        #逐列进行处理
        for eachzhibiao in range(0,5):
            zhibiaodata=[]
            for raw in ck_data:
                #
                tempvalue=raw[eachzhibiao+1]
                if tempvalue is None:
                    tempvalue=0
                zhibiaodata.append([raw[0],tempvalue,raw[6]])

            temp= get_drill_tb_hb(zhibiaodata, namedict, data['queryDate'], '', misskeyshow=True,misskeyvalue='-')

            for key, value in temp.items():
                atemp = {}
                for ikey, ivalue in value.items():
                    if ikey == 'value':
                        newkey = sd_zhibiao_list[eachzhibiao]
                    elif ikey == '同比去年':
                        newkey = sd_zhibiao_list[eachzhibiao]+'YoY'
                    else:
                        continue
                    atemp[newkey] = ivalue
                if key not in sd_sqldata:
                    sd_sqldata[key]=atemp
                else:
                    sd_sqldata[key].update(atemp)
    #支付
    sql = get_zf_info_sql_for_report(datacopy, sd_zf_indicator_table)

    conn_ck.execute(sql)
    ck_data = conn_ck.fetchall()
    zf_sqldata = {}
    if len(ck_data) > 0:
        namedict = {ele[0]: ele[0] for ele in ck_data}

        # 逐列进行处理
        for eachzhibiao in range(0, 5):
            zhibiaodata = []
            for raw in ck_data:
                #
                tempvalue = raw[eachzhibiao + 1]
                if tempvalue is None:
                    tempvalue = 0
                zhibiaodata.append([raw[0], tempvalue, raw[6]])

            temp = get_drill_tb_hb(zhibiaodata, namedict, data['queryDate'], '', misskeyshow=True,misskeyvalue='-')

            for key, value in temp.items():
                atemp = {}
                for ikey, ivalue in value.items():
                    if ikey == 'value':
                        newkey = zf_zhibiao_list[eachzhibiao]
                    elif ikey == '同比去年':
                        newkey = zf_zhibiao_list[eachzhibiao] + 'YoY'
                    else:
                        continue
                    atemp[newkey] = ivalue
                if key not in zf_sqldata:
                    zf_sqldata[key] = atemp
                else:
                    zf_sqldata[key].update(atemp)
    i=0
    rtn_sqldata={}
    for key in sd_sqldata.keys():
        if i==2:                     #只取两个key
            break
        rtn_sqldata[key]=sd_sqldata[key]
        if key in zf_sqldata.keys():

            rtn_sqldata[key].update(zf_sqldata[key])
        else:
            rtn_sqldata[key].update(dict(zip(zf_zhibiao_list+[ele+'YOY' for ele in zf_zhibiao_list],['-']*10)))
        i+=1
    return rtn_sqldata



def report_sql(data):
    '''web悟空实时报表-sql'''
    uv_info=report_sql_uv(data)
    sd_zf_info=report_sql_sdzf_info(data)

    for key in sd_zf_info.keys():
        if key in uv_info.keys():
            sd_zf_info[key].update(uv_info[key])
        else:
            sd_zf_info[key].update({'uv':0,'uvYoY':'-'})

    #组合
    return sd_zf_info


