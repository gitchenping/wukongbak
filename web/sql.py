from utils._sql import get_uv_sql_for_report,get_sd_info_sql_for_report,get_zf_info_sql_for_report
from  utils._sql import get_drill_tb_hb
from resources.map import bd_id_dict

app_id={'全部':"('2','3')",'IOS':"('2')",'安卓':"('3')"}
product_is_merchant={'自营':'(1)','招商':'(2)','全部':'(1,2)'}

bd_id={'出版物':'(1)','日百服':'(2)','数字':'(3)','文创':'(4)','其他':'(5)','全部':'(1,2,3,4,5)'}


def get_filters_where_for_reco(filters):
    '''推荐-单品分析筛选条件'''

    wheredict=dict(filters)
    where=''
    #日期
    data_date=wheredict['start']
    where += "data_date='" + data_date + "'"+" and "
    #平台
    appid=" in "+app_id[wheredict['platform']]
    where+="app_id "+ appid+ " and "
    #经营方式
    shoptype=" in "+product_is_merchant[wheredict['shop_type_name']]
    where+="product_is_merchant"+shoptype+ " and "
    #事业部
    bd =" in " +bd_id[wheredict['bd_id']]
    where+="bd_id "+bd+' and '
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
        where+="model_cn_name='"+module_name+"'"+ " and "

    return where.rstrip('and ')
    pass

'''推荐sql data'''
def get_sql_data_reco(data,indicator_cal_map,filters,conn_ck=None):
    # 获取筛选条件
    where = get_filters_where_for_reco(filters)
    if data!={}:
        date=data['日期']
        product_id = data['商品ID']
        product_name = data['商品名称']
        where += ' and product_id= ' + str(product_id) \
                 #+ " and product_name= '" + str(product_name) + "'"
    else:
        where+=" and product_id != -1 group by product_id"

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

        if sqldata['商品曝光pv']==0 and sqldata['商品点击pv']==0 and sqldata['收订金额']==0:
            return {}
        if data=={}:
            return sqldata

        # 平均曝光、平均点击
        expose_click_column = "count(DISTINCT CASE WHEN model_type = 1 THEN udid ELSE NULL END) as expose_uv," \
                              "count(DISTINCT CASE WHEN model_type = 3 THEN udid ELSE NULL END) as click_uv," \
                              "round(case when expose_uv!=0 then sum(max_expose_location) /expose_uv else null end) as avg_expose_location," \
                              "round(case when click_uv!=0 then sum(max_click_location) /click_uv else null end) as avg_click_location"
        expose_click_inner_sql = " select model_type,udid,MAX(CASE WHEN model_type = 1 then position else 0 end) AS max_expose_location," \
                                 "MAX(CASE WHEN model_type = 3 then position else 0 end) AS max_click_location from dm_report.reco_order_detail" \
                                 " where " + where + " and model_type in (1,3) and position>0 group by udid,model_type"

        expose_click_sql = " select " + expose_click_column + " from (" + expose_click_inner_sql + ") a"

        conn_ck.execute(expose_click_sql)
        ck_data = conn_ck.fetchall()
        if len(ck_data) > 0:
            sqldata['平均曝光位置'] = ck_data[0][-2]
            sqldata['平均点击位置'] = ck_data[0][-1]

    if data!={}:
        sqldata['日期']=date
        sqldata['商品ID']=product_id
        sqldata['商品名称']=product_name

    return sqldata

def report_sql_uv(data,reportname,conn_ck):
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

def report_sql_sdzf_info(data,reportname,conn_ck):
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


def report_sql(data,reportname='category',conn_ck=None):
    '''web悟空实时报表-sql'''
    uv_info=report_sql_uv(data,reportname,conn_ck)
    sd_zf_info=report_sql_sdzf_info(data,reportname,conn_ck)

    for key in sd_zf_info.keys():
        if key in uv_info.keys():
            sd_zf_info[key].update(uv_info[key])
        else:
            sd_zf_info[key].update({'uv':0,'uvYoY':'-'})

    #组合
    return sd_zf_info


def crm_sql_data(datakey,tableinfokey,sqlcursor,table,date,month):
    '''

    :param data: 筛选条件
    :return:
    '''
    if month:
        sql_date=date[0:7]+"-01"
    else:
        sql_date = date[0:4] + "-01-01"
    supplier_num=set()
    product_id=set()
    warehouse_name=set()

    for ele in datakey:
        supplier_warehousename_product = ele.split('_')
        if len(supplier_warehousename_product)==1:
            supplier_num.add(supplier_warehousename_product[0])

        elif len(supplier_warehousename_product)==2:
            supplier_num.add(supplier_warehousename_product[0])
            product_id.add(int(supplier_warehousename_product[1]))

        else:
            supplier_num.add(supplier_warehousename_product[0])
            warehouse_name.add(supplier_warehousename_product[1])
            product_id.add(int(supplier_warehousename_product[2]))

    #数据表字段
    column=','.join([ele for ele in tableinfokey])
    supplier_in=','.join(["'"+str(ele)+"'" for ele in supplier_num])
    warehouse_in=','.join(["'"+str(ele)+"'" for ele in warehouse_name])
    product_in=','.join([str(ele) for ele in product_id])

    where=''
    groupby=' group by '
    if supplier_in !='':
        where += " supplier_num in (" + supplier_in +')'
        groupby += " supplier_num"
    if warehouse_in !='':
        where += " and warehouse_name in (" + warehouse_in +")"
        groupby += " ,warehouse_name"
    if product_in !='':
        where += " and product_id in (" + product_in +")"
        groupby += " ,product_id"

    mysql_sql=" select "+column+" from "+table+" where "+where+" and data_date='"+sql_date+"'"+groupby

    sqlcursor.execute(mysql_sql)
    sqldata=sqlcursor.fetchall()

    sql_data={}
    for each in sqldata:
        data = dict(zip(tableinfokey, each))
        supplier_num = data.pop('supplier_num')
        key=supplier_num

        if data.__contains__('warehouse_name'):
            warehouse_name = data.pop('warehouse_name')
            key += '_' + str(warehouse_name)

        if data.__contains__('product_id'):
            product_id = data.pop('product_id')
            key+= '_' + str(product_id)

        sql_data[key] = data

    return sql_data



def sqldata(sql,cursor,offset=1000):
    '''

    :param sql: msql 查询
    :param cursor:
    :param offset: 偏移量
    :return:
    '''
    initoffset=0
    sampletimes=0
    while sampletimes<1:
        newsql = sql + " limit " + str(initoffset) + ","+str(int(offset/2))
        cursor.execute(newsql)
        row_data = cursor.fetchall()
        yield row_data
        sampletimes+=1
        initoffset+=offset
