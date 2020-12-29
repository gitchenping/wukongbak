from util import util

conn_ck = util.connect_clickhouse()

source_dict={'1':'主站','2':'天猫','4':'拼多多','3':'抖音'}
app_dict={'1':'安卓','2':'IOS'}
bd_dict={'1':'出版物','2':'日百','4':'文创'}
cat_name_dict={'1':'科技','2':'教育','3':'文艺','4':'生活','5':'社科','6':'经营','7':'音像','8':'原版','9':'童书','10':'others'}

def jingyingfenxi(data):

    datacopy=dict(data)

    filedstr=datacopy['field_str']
    if filedstr=='create_parent_amt':
        column="count(DISTINCT parent_id) AS create_parent_amt"
    elif filedstr=='priceByParent':
        column = "sum(bargin_price * order_quantity) / count(DISTINCT parent_id) AS priceByParent"
    elif filedstr=='priceByPerson':
        column = "sum(bargin_price * order_quantity) / count(DISTINCT cust_id) AS priceByPerson"
    elif filedstr=='create_order_amt':
        column = "count(DISTINCT order_id) AS create_order_amt"
    else:
        column='sum(bargin_price * order_quantity) AS create_price'

    datatype=datacopy['sale_type']
    if datatype=='sd':
        data_type='1'
    elif datatype=='zf':
        data_type = '2'
    else:
        data_type = '3'

    where='source in '
    source=datacopy['source']
    source_value=''
    if source=='all':
        source_value="('1','2','3','4')"
    else:
        source_value = "('"+source+"')"

    where+=source_value

    parentplatform=datacopy['parent_platform']
    platform = datacopy['platform']

    #由source、parentplatform 求platform

    platform_value=''
    if source == '1' and parentplatform == '3':
        platform_value = "('12','20')"
    elif source == '1' and parentplatform == '4':
        platform_value = "('0')"
    elif source=='1' and parentplatform=='1' and platform=='all':    #
        platform_value="('1','2')"
    elif source=='1' and parentplatform=='2' and platform=='all':
        platform_value = "('3','4','5','6','7','8','9')"
    else:
        platform_value="('"+platform+"')"

    platform_where=''
    if source=='1':
        platform_where=" and platform in "+platform_value

    where+=platform_where

    shoptype=datacopy['shop_type']
    shop_type_value=''
    if shoptype!='all':
        shop_type_value = " and shop_type in ('"+shoptype+"')"

    where += shop_type_value

    bdid = datacopy['bd_id']
    bd_id_value=''
    if bdid != 'all':
        bd_id_value = " and bd_id in ('" + bdid + "')"
    else:
        bd_id_value=" and bd_id in ('1','2','4')"

    where+=bd_id_value

    day_hour_str=datacopy['date'].split(' ')
    hour_datatype=[day_hour_str[0],day_hour_str[1],data_type]
    where=where+" and  date_str in ('{}') and hour_str <= '{}' and data_type in ('{}') ".format(*hour_datatype)


    drill_data={}

    # trend
    trend = {}  # 存放trend 结果

    trendsql = " select  hour_str,"+column+" from bi_mdata.kpi_order_info_all "+ \
        "where "+where+" group by hour_str"
    # ans = client.execute(trendsql)
    ck_data=conn_ck.execute(trendsql)

    if len(ck_data) > 0 and ck_data[0][0] is not None:  # key值换算
        for ele in ck_data:
            key = ele[0]+"点"
            value = ele[1]
            trend[key] = round(value, 2)
    drill_data['trend'] = trend

    #事业部分布
    bd={}

    if datacopy['bd_id']=='1':
        column_bd='cat_id,'
        group_by=" group by cat_id"
        bdnamedict=cat_name_dict
    else:
        column_bd='bd_id,'
        group_by=" group by bd_id"
        bdnamedict=bd_dict

    bdsql = " select  "+column_bd+column+" from bi_mdata.kpi_order_info_all "+ \
        "where "+where+group_by

    ck_data = conn_ck.execute(bdsql)

    if len(ck_data) > 0 :  # key值换算
        for ele in ck_data:
            key = bdnamedict[ele[0]]
            value = ele[1]
            if value is None:
                value = 0
            # trenddict[key] =int(Decimal(ans[0][0]).quantize(Decimal(1)))
            bd[key] = round(value, 2)
    drill_data['bd'] = bd

    #平台分布
    platform={}
    if source not in ['2','3','4']:    #天猫、抖音、拼多多下钻页没有平台分布

        if parentplatform not in ['2','3','4']: #轻应用\H5\PC没有平台分布

            if parentplatform=='all':
                column_plat='source,'
                group_by=" group by source"
                platdict=source_dict
            else:
                if platform not in ['1', '2']:  # 安卓、IOS没有平台分布
                    group_by=''
                else:
                    column_plat = 'platform,'
                    group_by=" group by platform"
                    platdict=app_dict

            if len(group_by)>0:

                platsql="select "+column_plat+column+" from bi_mdata.kpi_order_info_all "+ \
                            "where " + where + " "+group_by

                ck_data = conn_ck.execute(platsql)

                if len(ck_data) > 0 :  # key值换算

                        for ele in ck_data:
                            key = platdict[ele[0]]
                            value = ele[1]
                            if value is None:
                                value = 0
                            # trenddict[key] =int(Decimal(ans[0][0]).quantize(Decimal(1)))
                            platform[key] = round(value, 2)
                drill_data['platform'] = platform

    #新老客分布
    customer={}
    customersql="select new_name,"+column+" FROM kpi_order_info_all "+ \
                "where " + where + " group by new_name"

    ck_data = conn_ck.execute(customersql)

    if len(ck_data) > 0 :  # key值换算
        for ele in ck_data:
            key=ele[0]
            value=ele[1]
            if value is None:
                value=0
            # trenddict[key] =int(Decimal(ans[0][0]).quantize(Decimal(1)))
            customer[key] = round(value, 2)
    drill_data['customer'] = customer

    #城市
    city={}
    citysql = "select city_id,"+column+" FROM kpi_order_info_all " + \
                  "where " + where + " group by city_id order by "+filedstr+" limit 0,10"


    # ck_data = conn_ck.execute(citysql)
    ck_data = []
    if len(ck_data) > 0:  # key值换算
        for ele in ck_data:
            key=ele[0]
            value=ele[1]
            if value is None:
                value=0
            # trenddict[key] =int(Decimal(ans[0][0]).quantize(Decimal(1)))
            city[key] = round(value, 2)
        drill_data['city'] = city

    return drill_data