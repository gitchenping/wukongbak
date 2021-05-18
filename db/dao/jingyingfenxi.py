from utils import util
from ._sql import get_bd_where,get_platform_where,get_time_where,get_shoptype_where,get_eliminate_where,get_drill_tb_hb
from utils import tb_hb
from utils.date import get_trend_where_date

from resources.map import customer_dict


def sql_jingyingfenxi_overview(data,table_dict,conn_ck=None):
    '''
    :param data:  api参数
    :param table: ck表
    :return:
    '''
    datacopy=dict(data)
    saletype=datacopy['sale_type']
    datetype=datacopy['date_type']
    date=datacopy['date_str']
    shoptype=datacopy['shop_type']

    sd_zf_ck = datacopy['sale_type']
    zhibiao=[]
    groupby=''
    if sd_zf_ck == "sd":
        name = "收订"
        zhibiao=['金额','订单量','单均价','客单价','包裹数','取消率']

    elif sd_zf_ck == 'zf' :
        name = "支付"
        zhibiao = ['金额', '订单量', '单均价', '客单价', '包裹数', '实付金额','净销售额']

    else:
        name = "出库"
        zhibiao = ['金额', '订单量', '单均价', '客单价', '包裹数', '实付金额', '净销售额','毛利额','毛利率']
        if shoptype == '2':
            zhibiao = ['金额', '订单量', '单均价', '客单价', '包裹数', '实付金额', '净销售额']

    #金额、订单量、单均价、客单价、包裹数、实付金额、净销售额
    column='''
            sumMerge(prod_sale_amt_state) as create_price,
            groupBitmapMerge( parent_id_state) as create_parent_amt,
            sumMerge(prod_sale_amt_state)/groupBitmapMerge( parent_id_state) as priceByParent,
            sumMerge(prod_sale_amt_state)/groupBitmapMerge(cust_id_state) as priceByPerson,
            groupBitmapMerge(order_id_state) as create_order_amt,
            sumMerge(out_pay_amount_state) as out_pay_amount,
            sumMerge(out_profit_state) as out_profit 
    '''

    if saletype =='ck' and shoptype!='2' :           #出库计算毛利额、毛利率
        column+=",sumMerge(gross_profit_state) as gross_profit" \
                ",sumMerge(gross_profit_state)/sumMerge(out_profit_state) as gross_profit_rate"


    keylist = ['value', '环比',  '同比去年']
    if datetype=='day' or datetype=='d':
        column+=',toString(date_str)'
        keylist = ['value', '环比', "同比上周", '同比去年']
    elif datetype=='mtd' or datetype=='m':
        column += ',toString(toStartOfMonth(toDate(date_str)) as date_str) as _date_str'

    elif datetype=='qtd' or datetype=='q':
        column += ',toString(toStartOfQuarter(toDate(date_str)) as date_str) as _date_str'
    else:
        column += ',toString(toStartOfWeek(toDate(date_str),1) as date_str) as _date_str'


    #筛选条件
    where=''
    where+=" where "+get_platform_where(datacopy)+get_bd_where(datacopy)+get_shoptype_where(datacopy)+get_eliminate_where(datacopy)
    where+=get_time_where(data)

    groupby = " group by date_str order by date_str desc"

    sql=" select "+column+" from "+table_dict[saletype]+where+groupby
    #执行sql
    try:
        conn_ck.execute(sql)
    except Exception as e:
        print(sql)
    rawdata=conn_ck.fetchall()

    sqldata={}
    keylistset=set(keylist)
    can_cal_tb_hb =True
    if len(rawdata)>0:

        #同比环比键值
        tb_hb_key_list=tb_hb.get_tb_hb_key(rawdata,date,datetype)
        if len(tb_hb_key_list)>0:           #可以进行同环比计算
            newdata=tb_hb.tb_hb_cal(rawdata)

            for col in range(len(newdata[0]) - 1):
                temp=[]
                for raw in newdata:
                    temp.append(raw[col])
                temp=[ele if ele!=0 else '--' for ele in temp ]

                zhibiao_name=zhibiao[col]
                key_name=zhibiao_name
                if zhibiao_name not in ['毛利额','毛利率']:
                    key_name=name+zhibiao_name
                sqldata[key_name]= dict(zip(keylist,temp))

                # for key in keylistset-set(tb_hb_key_list):
                #     sqldata[key_name].update({key:'--'})
    else:                                         #没有数据，各指标为{}
         for ele in zhibiao:

             if ele not in ['毛利额', '毛利率']:
                 sqldata[name+ele]={'value':'--'}
             else:
                sqldata[ele] = {'value':'--'}
    return sqldata


def jingyingfenxi_drill(data,tabledict,columndict,conn_ck=None):

    is_cal_trend=True
    is_cal_bd=True
    is_cal_platform=True
    is_cal_customer=True
    is_cal_city=False

    datacopy = dict(data)
    filedstr = datacopy['field_str']

    source = datacopy['source']
    parentplatform = datacopy['parent_platform']
    datetype = datacopy['date_type']
    sd_zf_ck = datacopy['sale_type']
    date=datacopy['date_str']

    cal_zf_cancel_need=False

    if sd_zf_ck=="zf" and filedstr=='cancel_rate':      #支付有支付取消率
        cal_zf_cancel_need=True

    column=columndict[filedstr]

    if datetype == 'day' or datetype == 'd':
        column += ',toString(date_str) as _date_str'
        groupby_new_flag='day_new_flag'
    elif datetype == 'mtd' or datetype == 'm':
        column += ',toString(toStartOfMonth(toDate(date_str))) as _date_str'
        groupby_new_flag = 'month_new_flag'
    elif datetype == 'qtd' or datetype == 'q':
        column += ',toString(toStartOfQuarter(toDate(date_str))) as _date_str'
        groupby_new_flag = 'quarter_new_flag'
    else:
        column += ',toString(toStartOfWeek(toDate(date_str),1)) as _date_str'
        groupby_new_flag = 'week_new_flag'

    # 筛选条件
    where = ''
    where += " where " + get_platform_where(datacopy) + get_bd_where(datacopy) + get_shoptype_where(
        datacopy) + get_eliminate_where(datacopy)
    # where += get_time_where(data)

    _groupby = " group by _date_str"
    _orderby=" order by _date_str desc"

    drill_data={}                                          #存放当前下钻页所有指标数据
    # trend
    if is_cal_trend:
        trend = {}  # 存放trend 结果

        wheredata=get_trend_where_date(data)

        trendsql=" select "+column+" from "+tabledict[sd_zf_ck]+" "+where+wheredata + _groupby

        conn_ck.execute(trendsql)
        ck_data = conn_ck.fetchall()

        temp=[]
        if cal_zf_cancel_need:
            trendsql = " select " + column + " from " + "bi_mdata.dm_order_cancel_day" + " " + where + wheredata + _groupby
            conn_ck.execute(trendsql)
            ck_data_2 = conn_ck.fetchall()

            ck_data=[list(raw) for raw in ck_data]

            for i in range(len(ck_data_2)):
                canceldate=ck_data_2[i][1]
                for raw in ck_data:
                    if raw[1]==canceldate:
                        if raw[0] != 0:
                            temp.append([ck_data_2[i][0] / raw[0] * 100, raw[-1]])
                        break
        else:
            temp = ck_data

        if len(temp) > 0:
            for raw in temp:
                key=util.get_trendkey(datetype,raw[1])
                if raw[0] is not None:
                    trend[key]=round(raw[0],2)
                else:
                    trend[key]='--'

        drill_data['trend']=trend

    #bd分布
    if is_cal_bd:
        bd={}
        if datacopy['bd_id']!='all':
                namedict = map.catgory_path_dict
                column_bd='case'
                for key in namedict.keys():
                    column_bd+=" when category_path3 in "+str(namedict[key])+" then '"+key+"'"
                #others
                column_bd+=" else '10' end as _bd_id,"
                group_by=" group by _bd_id,_date_str"
                bdnamedict= map.cat_name_dict

        else:
                column_bd="CASE WHEN bd_id IN (5, 12) THEN 1 " \
                          "WHEN bd_id IN (1, 4, 9, 15, 16) THEN 2 " \
                          "WHEN bd_id IN (3) THEN 6 "\
                          "WHEN bd_id IN (6) THEN 3 " \
                          "WHEN bd_id IN (20, 21, 23) THEN 4 ELSE 5 END AS _bd_id,"

                group_by = " group by "+ column_bd +"_date_str"
                bdnamedict = map.bd_id_dict

        order_by=" order by _bd_id,_date_str desc"

        where += get_time_where(data)

        bdsql = " select  "+column_bd+column+ " from " +tabledict[sd_zf_ck]+" "+where + group_by+order_by

        conn_ck.execute(bdsql)
        ck_data=conn_ck.fetchall()

        temp = []
        if cal_zf_cancel_need:
            trendsql = " select " + column_bd+column + " from " + "bi_mdata.dm_order_cancel_day" + " " + where + group_by+order_by
            conn_ck.execute(trendsql)
            ck_data_2 = conn_ck.fetchall()

            ck_data = [list(raw) for raw in ck_data]

            for i in range(len(ck_data_2)):
                canceldate = ck_data_2[i][2]
                for raw in ck_data:
                    if raw[-1] == canceldate and raw[0]==ck_data_2[i][0]:
                        if raw[1]!=0:
                            temp.append([raw[0],ck_data_2[i][1] / raw[1]*100,raw[-1]])
                        break
        else:
            temp=ck_data

        if len(temp) > 0:  # key值换算
            bd=get_drill_tb_hb(temp, bdnamedict,date,datetype)
        drill_data['bd'] = bd

    ##平台分布
    if is_cal_platform:
        platform={}
        show_platform=True
        # where+=get_time_where(data)
        if source not in ['2','3','4']:    #天猫、抖音、拼多多下钻页没有平台分布

            if parentplatform not in ['2','3','4']: #轻应用\H5\PC没有平台分布

                if parentplatform=='all':
                    column_plat='source'
                    group_by=_groupby+",source"
                    platdict= map.source_dict
                else:
                    if platform not in ['1', '2']:  # 安卓、IOS没有平台分布
                        group_by=''
                    else:
                        column_plat = 'platform'
                        group_by=_groupby+",platform"
                        platdict= map.app_dict

                if len(group_by)>0:
                    order_by=" order by "+column_plat+","+"_date_str desc"
                    platsql="select "+column_plat+","+column+ " from " +tabledict[sd_zf_ck]+" "+where + group_by+order_by

                    conn_ck.execute(platsql)
                    ck_data =conn_ck.fetchall()

                    temp = []
                    if cal_zf_cancel_need:
                        trendsql = " select " + column_plat + "," + column + " from " + "bi_mdata.dm_order_cancel_day" + " " + where + group_by + order_by
                        conn_ck.execute(trendsql)
                        ck_data_2 = conn_ck.fetchall()

                        ck_data = [list(raw) for raw in ck_data]

                        for i in range(len(ck_data_2)):
                            canceldate = ck_data_2[i][2]
                            for raw in ck_data:
                                if raw[-1] == canceldate and raw[0] == ck_data_2[i][0]:
                                    if raw[1] != 0:
                                        temp.append([raw[0], ck_data_2[i][1] / raw[1] * 100, raw[-1]])
                                    break
                    else:
                        temp = ck_data

                    if len(temp) > 0 :                                               # key值换算
                        platform = get_drill_tb_hb(temp, platdict,date,datetype)

            else:
                 show_platform=False
                 platform={}
        else:
             show_platform=False
             platform={}

        if not show_platform:
            pass
        else:
            drill_data['platform'] = platform

    #新老客分布
    if is_cal_customer:
        customer = {}
        order_by = " order by "+groupby_new_flag+"," + "_date_str desc"
        group_by=_groupby+","+groupby_new_flag
        customersql = "select "+groupby_new_flag+"," +column+ " from " +tabledict[sd_zf_ck]+" "+where + group_by+order_by

        conn_ck.execute(customersql)
        ck_data = conn_ck.fetchall()

        temp = []
        if cal_zf_cancel_need:
            trendsql = " select " + groupby_new_flag+"," + column + " from " + "bi_mdata.dm_order_cancel_day" + " " + where + group_by + order_by
            conn_ck.execute(trendsql)
            ck_data_2 = conn_ck.fetchall()

            ck_data = [list(raw) for raw in ck_data]

            for i in range(len(ck_data_2)):
                canceldate = ck_data_2[i][2]
                for raw in ck_data:
                    if raw[-1] == canceldate and raw[0] == ck_data_2[i][0]:
                        if raw[1]!=0:
                            temp.append([raw[0], ck_data_2[i][1] / raw[1] * 100, raw[-1]])
                        break
        else:
            temp = ck_data

        if len(temp) > 0:                                            # key值换算
            customer = get_drill_tb_hb(temp, map.customer_dict,date,datetype)
        drill_data['customer'] = customer

    return drill_data
    pass


def sql_jingyingfenxi_drill(data,tabledict):
    datacopy = dict(data)

    # date = util.datechange(datetype, datacopy['date_str'])
    sd_zf_ck = datacopy['sale_type']
    if sd_zf_ck == "sd":
        name = "收订"
    elif sd_zf_ck == 'zf':
        name = "支付"
    else:
        name = "出库"
    shoptype=datacopy['shop_type']

    # 所有首页可以下钻的指标
    drill_dict = {}
    drill_dict.update(map.drill_common_dict)
    if sd_zf_ck != 'ck':  # 收订和支付有取消率
        drill_dict.update(map.drill_cancel_dict)
    else:
        drill_dict.update(map.drill_ck)
    if sd_zf_ck != 'sd':
        drill_dict.update(map.drill_zf_ck_dict)
    #各指标下钻的计算逻辑
    columndict = {
            'create_price': "sumMerge(prod_sale_amt_state) as create_price",
            'priceByPerson': "sumMerge(prod_sale_amt_state)/groupBitmapMerge(cust_id_state) as priceByPerson",
            'priceByParent': "sumMerge(prod_sale_amt_state)/groupBitmapMerge( parent_id_state) as priceByParent",
            'create_order_amt': "groupBitmapMerge(order_id_state) as create_order_amt",
            'out_pay_amount': "sumMerge(out_pay_amount_state) as out_pay_amount",
            'create_parent_amt': "groupBitmapMerge( parent_id_state) as create_parent_amt",
            'out_profit': "sumMerge(out_profit_state) as out_profit",
            'gross_profit': "sumMerge(gross_profit_state) as gross_profit",
            'gross_profit_rate': "sumMerge(gross_profit_state)/sumMerge(out_profit_state)*100 as gross_profit_rate",
            'cancel_rate': "groupBitmapMerge( parent_id_state) as create_parent_amt"
    }

    #本次不考虑转化率
    drill_dict.pop('transRate')

    sql_drill_data = {}

    if sd_zf_ck == 'ck' and shoptype == '2':
        drill_dict.pop('gross_profit')
        drill_dict.pop('gross_profit_rate')

    for field in drill_dict.keys():
    # for field in ['cancel_rate']:
        datacopy['field_str']=field
        sql_drill_data[name+drill_dict[field]]=jingyingfenxi_drill(datacopy,tabledict,columndict,conn_ck=None)
    return sql_drill_data
    pass