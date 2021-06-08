from ._sql import get_uv_sql_for_report,get_where_for_report
from ._sql import get_drill_tb_hb
from utils import util
from resources.map import bd_id_dict

#sql 链接
conn_ck = util.connect_clickhouse(host='10.7.30.148')

sd_zf_report_map={
    #指标
    'sd_zf_amount':"SUM(case when data_type='{}'then bargin_price * order_quantity else null end) AS amount",   #金额
    'sd_zf_package':"COUNT(DISTINCT case when data_type='{}'then order_id else null end) AS package",     #包裹量
    'sd_zf_customer':"COUNT(DISTINCT case when data_type='{}'then cust_id else null end) AS customer",       #人数
    'sd_zf_new_customer':"COUNT(DISTINCT CASE WHEN data_type='{}' and new_id = '1' then cust_id else null end) AS new_customer" #新客
}

def get_uv_sql_for_report(data,tablename,reportname='category'):
    '''实时报表-uv'''
    where,column_category=get_where_for_report(data)

    if reportname=='category':
        if 'category_path2' not in where:
            uvwhere = where
        else:
            uvwhere=where+" and category != '"+data['categoryPath']+"'"+" and category !='' "
    else:
        uvwhere = where

    column=''
    orderbyname=''
    if reportname=='bussiness':
        orderbyname="_bd_id"
        column="case when bd_id_prod in (5,12) then '1'" \
               " when bd_id_prod in (1,4,9,15,16) then '2'" \
               " when bd_id_prod in (3) then '6'" \
               " when bd_id_prod in (20,21,23) then '4'" \
               " when bd_id_prod in (6) then '3'" \
               " else '5' end "+ " as "+orderbyname
    else:
        orderbyname="category"
        column=column_category+" as "+orderbyname
    column+=',COUNT(DISTINCT device_id) AS UV,toString(date_str) as date_str'

    order_by=" order by "+ orderbyname+",date_str desc"

    sql=" select "+column+" from "+tablename+" where "+uvwhere+" group by "+orderbyname+",date_str "+order_by
    return sql



def get_sd_info_sql_for_report(data,tablename,reportname):
    '''实时报表收订-其他指标'''
    where, column_category = get_where_for_report(data,False)

    where += " and data_type in ('1','4')"


    if reportname!='bussiness':       #品类
        if 'category_path2' not in where:
            sdwhere = where
        else:
            sdwhere = where + " and category != '" + data['categoryPath'] + "'" + " and category !='' "

        sd_zf_category = column_category + " as category"
        column=sd_zf_category+","
        groupby_orderby=" group by "+"category,date_str"+" order by category,date_str desc limit 4"
    else:
        sdwhere = where

        column = "bd_id" + ","
        groupby_orderby = " group by " + "bd_id,date_str" + " order by bd_id,date_str desc"

    for cloumn_key in sd_zf_report_map.keys():
        column+=sd_zf_report_map[cloumn_key].format('1')+","

    sd_zf_order="COUNT(DISTINCT case when data_type = '1' then  parent_id else null end) AS order_sum"

    #收订
    sd_cancel = "COUNT(DISTINCT case when data_type='4' then parent_id else null end) AS order_cancel"
    sd_cancel_rate="(case when order_sum!=0 then order_cancel /order_sum *100 else null end ) as sd_cancel_rate"

    column+=sd_cancel_rate+",date_str,"+sd_zf_order+","+sd_cancel

    sd_sql="select "+column+ \
           " from "+tablename+ \
           " where "+sdwhere+groupby_orderby
    return sd_sql


def get_zf_info_sql_for_report(data,tablename,reportname):
    '''实时报表支付-其他指标'''
    where, column_category = get_where_for_report(data,False)

    where += " and data_type in ('2','4')"

    sd_zf_order = "COUNT(DISTINCT case when data_type = '2' then  parent_id else null end) AS order_sum"
    if reportname != 'bussiness':  # 品类
        if 'category_path2' not in where:
            zfwhere = where
        else:
            zfwhere = where + " and category != '" + data['categoryPath'] + "'" + " and category !='' "

        sd_zf_category = column_category + " as category"
        column = sd_zf_category + ","

        for cloumn_key in sd_zf_report_map.keys():
            column += sd_zf_report_map[cloumn_key].format('2') + ","
        column += "date_str," + sd_zf_order

        a_sql = " select " + column + " from " + tablename + " where " + zfwhere + " group by " + "category,date_str"

        b_sql = " select COUNT(DISTINCT parent_id) as cancel_num,date_str,category from (" + \
                "select parent_id,COUNT(DISTINCT data_type ) AS num,date_str," + column_category + " as category" + \
                " from " + tablename + " where " + zfwhere + " group by parent_id,date_str,category having num>1) a" + \
                " group by category,date_str"
        groupbyname = "category"
        groupby_orderby = " order by category,date_str desc limit 4"
    else:
        zfwhere = where
        column = "bd_id" + ","

        for cloumn_key in sd_zf_report_map.keys():
            column += sd_zf_report_map[cloumn_key].format('2') + ","
        column += "date_str," + sd_zf_order

        a_sql = " select " + column + " from " + tablename + " where " + zfwhere + " group by " + "bd_id,date_str"

        b_sql = " select COUNT(DISTINCT parent_id) as cancel_num,date_str,bd_id from (" + \
                "select parent_id,COUNT(DISTINCT data_type ) AS num,date_str," + " bd_id" + \
                " from " + tablename + " where " + zfwhere + " group by parent_id,date_str,bd_id having num>1) a" + \
                " group by bd_id,date_str"
        groupbyname="bd_id"
        groupby_orderby=" order by bd_id,date_str desc"

    zf_sql = "select " +groupbyname+" ,amount,package,customer,new_customer, " + \
            "case when a.order_sum!=0 then b.cancel_num /a.order_sum* 100 else null end as zf_cancel_rate,date_str "+ \
             " from (" +a_sql  +") a left join ("+ \
             b_sql+") b"+" on a."+groupbyname+" = b."+groupbyname+" and a.date_str = b.date_str"+ groupby_orderby

    return zf_sql




def report_sql_uv(data,reportname):
    # uv table
    uv_table = 'bi_mdata.mdata_flows_user_realtime_all'
    datacopy=dict(data)

    sql=get_uv_sql_for_report(datacopy,uv_table,reportname)
    conn_ck.execute(sql)
    ck_data = conn_ck.fetchall()
    sqldata = {}
    if len(ck_data) > 0:
        if reportname=="category":
            namedict = {ele[0]: ele[0] for ele in ck_data}
        else:
            namedict={key:value+"事业部" for key,value in bd_id_dict.items()}
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

def report_sql_sdzf_info(data,reportname):
    # sd zf table
    sd_zf_indicator_table = 'bi_mdata.kpi_order_info_all_v2'
    sd_zhibiao_list=['subsAmount','subsPackages','subsCustomer','subsNewCustomer','subsCxlRate']
    zf_zhibiao_list=['pymtAmount','pymtPackages','pymtCustomer','pymtNewCustomer','pymtCxlRate']
    datacopy = dict(data)

    #收订
    sql = get_sd_info_sql_for_report(datacopy, sd_zf_indicator_table,reportname)

    conn_ck.execute(sql)
    ck_data = conn_ck.fetchall()
    sd_sqldata = {}
    if len(ck_data) > 0:
        if reportname=="category":
            namedict = {ele[0]: ele[0] for ele in ck_data}
        else:
            namedict={key:value+"事业部" for key,value in bd_id_dict.items()}

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
    sql = get_zf_info_sql_for_report(datacopy, sd_zf_indicator_table,reportname)

    conn_ck.execute(sql)
    ck_data = conn_ck.fetchall()
    zf_sqldata = {}
    if len(ck_data) > 0:
        if reportname=="category":
            namedict = {ele[0]: ele[0] for ele in ck_data}
        else:
            namedict={key:value+"事业部" for key,value in bd_id_dict.items()}

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
        if i==4:                     #只取两个key
            break
        rtn_sqldata[key]=sd_sqldata[key]
        if key in zf_sqldata.keys():

            rtn_sqldata[key].update(zf_sqldata[key])
        else:
            rtn_sqldata[key].update(dict(zip(zf_zhibiao_list+[ele+'YOY' for ele in zf_zhibiao_list],['-']*10)))
        i+=1
    return rtn_sqldata


def report_sql(data,reportname='category'):
    '''web悟空实时报表-sql'''
    uv_info=report_sql_uv(data,reportname)
    sd_zf_info=report_sql_sdzf_info(data,reportname)

    for key in sd_zf_info.keys():
        if key in uv_info.keys():
            sd_zf_info[key].update(uv_info[key])
        else:
            sd_zf_info[key].update({'uv':0,'uvYoY':'-'})

    #组合
    return sd_zf_info