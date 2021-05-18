from ._sql import get_uv_sql_for_report,get_sd_info_sql_for_report,get_zf_info_sql_for_report
from ._sql import get_drill_tb_hb
from utils import util
from resources.map import bd_id_dict

#sql 链接
conn_ck = util.connect_clickhouse(host='10.7.30.148')

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