from utils.date import get_tb_hb_date,get_month_end_day
from utils.tb_hb import tb_hb_cal,get_tb_hb_key

def get_platform_where(data,yinhao=False):
    '''获取platform对应的过滤条件,库不一样，字段代码在sql中可能需要加引号'''

    source = data['source']
    parentplatform = data['parent_platform']
    platform = data['platform']
    #
    if yinhao:          #默认不加
        _yin="'"
    else:
        _yin=""

    sourcewhere = ''
    if source == 'all':
        sourcelist=['1','2','3','4']
    else:
        sourcelist=[source]
    sourcewhere = "(" + ','.join([_yin + ele + _yin for ele in sourcelist]) + ")"
    #
    platformwhere = ''
    if source == '1' and parentplatform == '3':
        platformlist=['12','20']

    elif source == '1' and parentplatform == '4':
        platformlist = ['0']

    elif source == '1' and parentplatform == '1' and platform == 'all':  #
        platformlist = ['1', '2']

    elif source == '1' and parentplatform == '2' and platform == 'all':
        platformlist = ['3', '4','5','6','7','8','9']

    elif source=='1' and parentplatform=='all':
        platformlist = ['0','1','2','3', '4', '5', '6', '7', '8', '9','12','20']

    else:
        platformlist = [platform]

    platformwhere = "(" + ','.join([_yin + ele + _yin for ele in platformlist]) + ")"

    if source=='1':
        platformwhere=" and platform in "+platformwhere
    else:
        platformwhere = ''

    return " source in "+sourcewhere+platformwhere


def is_platform_show(data):
    '''下钻页平台是否显示'''
    datacopy=dict(data)

    source = datacopy['source']
    parentplatform = datacopy['parent_platform']
    platform = datacopy['platform']

    show=True                                               #默认显示
    if source not in ['2', '3', '4']:                       # 天猫、抖音、拼多多下钻页没有平台分布
        if parentplatform not in ['2', '3', '4']:           # 轻应用\H5\PC没有平台分布

            if parentplatform != 'all':
                if platform =='all':                    # 安卓、IOS没有平台分布
                    show=True
                else:
                    show=False
            else:
                show=True
        else:
            show = False
    else:
        show= False
    return show


def get_bd_where(data,yinhao=False):
    '''获取bd对应的过滤条件'''
    bd = data['bd_id']
    if yinhao:  # 默认不加
        _yin = "'"
    else:
        _yin = ""
    if bd == '1':                    #出版物
        bd_id = [5,12]
    elif bd == '2':                 #百货
        bd_id = [1,4,9,15,16]
    elif bd == '3':                 #数字
        bd_id = [6]
    elif bd == '4':                 #文创
        bd_id = [20,21,23]
    elif bd=='5':                   #其他
        bd_id = [1,3,4,9,15,16,5,12,6,20,21,23]
    elif bd=='6':                   #服装
        bd_id = [3]
    else:
        bd_id =''                    #[1, 3, 4, 5,6, 9, 12, 15, 16, 20, 21, 23

    if bd == '5':
        bdwhere = " and bd_id not in "
    else:
        bdwhere=" and bd_id in "

    if bd_id !='':

        bdwhere += "(" + ','.join([_yin + str(ele) + _yin for ele in bd_id]) + ")"
    else:
        bdwhere=''
    return bdwhere

def get_shoptype_where(data,yinhao=False):
    '''获取shoptype对应的过滤条件'''
    shoptype= data['shop_type']

    if yinhao:  # 默认不加
        _yin = "'"
    else:
        _yin = ""

    if shoptype == '1':
        shop_type = [1]
    elif shoptype == '2':
        shop_type = [2]
    else:
        shop_type = ''
    if shop_type !='':
        shoptypewhere=' and shop_type in '

        shoptypewhere += "(" + ','.join([_yin + str(ele) + _yin for ele in shop_type]) + ")"
    else:
        shoptypewhere=''

    return shoptypewhere


def get_eliminate_where(data):
    '''获取shoptype对应的过滤条件'''
    jianggong= data['eliminate_type']

    if jianggong=='all':
        jianggong_type=''
    elif jianggong == '1':
        jianggong_type = "is_jiangong = 2 "
    elif jianggong == '2':
        jianggong_type = "bigorder_flag = 2 "
    elif jianggong == '3':
        jianggong_type = "risk_flag = 2 "
    elif jianggong=='4':
        jianggong_type="order_type != '28' "
    elif jianggong=='5':
        jianggong_type="order_type NOT IN('29','30') "
    else:
        jianggong_type = "is_jiangong = 2 AND bigorder_flag = 2 AND risk_flag = 2 AND order_type NOT IN ('28','29','30') "

    jianggongwhere=''
    if jianggong_type!='':
        jianggongwhere = " and  " + jianggong_type
    return jianggongwhere

'''返回sql筛选的时间（环比、同比）条件'''
def get_time_where(data):
    '''
    :param data: 字典
    :return:
    '''
    datetype=data['date_type']
    date=data['date']

    datewhere=''
    tb_hb_date = get_tb_hb_date(date, datetype)

    if len(tb_hb_date)==4:     #天返回环比、周同比、年同比

        today=tb_hb_date[0]
        yesterday=tb_hb_date[1]
        last_week_day=tb_hb_date[2]
        last_year_day=tb_hb_date[3]

        datewhere += ' and t.date_str in ' + "('" + today+"','"+yesterday+"','"+last_week_day+"','"+last_year_day+"')"

    else:                   #周、月、季返回环比、同比去年
        hb_date=tb_hb_date[0]
        tb_date=tb_hb_date[1]

        for ele in tb_hb_date:
            if ele[0] is not None:
                 datewhere+= " t.date_str between '"+ele[0]+"' and '"+ele[1]+"' or "
        datewhere=" and ("+datewhere.strip('or ')+" )"
    return datewhere

#模块首页同环比计算
def get_overview_tb_hb(ck_data,name,date,datetype,misskeyshow=True,misskeyvalue='--'):
    '''

    :param ck_data: 二维列表，
    :param name: 指标名称
    :param date:
    :param datetype:
    :param misskeyshow:
    :param misskeyvalue:
    :return:
    '''
    tb_hb_key_list, default_tb_hb_key_list = get_tb_hb_key(ck_data, date, datetype)

    tempdict = {}

    if len(tb_hb_key_list)==0:
        tempdict[name]={'value':'--'}

    else :                       # 可以进行同环比计算
        newdata = tb_hb_cal(ck_data, misskeyvalue)

        temp_2 = []

        for col in range(len(newdata[0]) - 1):

            for raw in newdata:
                temp_2.append(raw[col])

        tempdict[name] = dict(zip(tb_hb_key_list, temp_2))

        # 不能计算的同比、环比key，其值设置为指定值
        if misskeyshow:
            nokey = set(default_tb_hb_key_list) - set(tb_hb_key_list)
            if len(nokey) > 0:
                for ele in nokey:
                    tempdict[name].update({ele: misskeyvalue})
        #
    return tempdict



def get_drill_tb_hb(ck_data,name_dict,date,datetype,misskeyshow=True,misskeyvalue='--'):
    '''
                            下钻页各指标同比环比计算
    :param ck_data: 二维列表,第一列作为下钻分类
    :param name_dict: 字典
    :param date: 时间字符串
    :param datetype: 时间类型
    :return: 同环比
    '''
    tempdict={}
    temp = []
    i = 0
    begin_index = ck_data[i][0]

    is_need_cal = False
    length = len(ck_data)
    while i <= length - 1:

        if begin_index == ck_data[i][0]:
            temp.append(ck_data[i][1:])  # 取值和时间
            key = str(begin_index)
            i += 1
        else:
            begin_index = ck_data[i][0]
            is_need_cal = True

        if i == length:
            is_need_cal = True

        if is_need_cal:
            # 计算环比、同比
            tb_hb_key_list,default_tb_hb_key_list = get_tb_hb_key(temp, date, datetype)

            if len(tb_hb_key_list) > 0:  # 可以进行同环比计算
                newdata = tb_hb_cal(temp,misskeyvalue)

                temp_2 = []
                for col in range(len(newdata[0])-1):

                    for raw in newdata:
                        temp_2.append(raw[col])

                name = name_dict[key]
                tempdict[name] = dict(zip(tb_hb_key_list, temp_2))

                #没有的key，值设置为'--'
                if misskeyshow:
                    nokey=set(default_tb_hb_key_list)-set(tb_hb_key_list)
                    if len(nokey)>0:
                        for ele in nokey:
                            tempdict[name].update({ele:misskeyvalue})
                #
                temp = []
                is_need_cal = False
            else:
                temp=[]
                is_need_cal = False
    return tempdict

def get_where_for_report(data,uv=True):
    '''实时报表字段、筛选条件'''
    category=data['categoryPath']
    category_list=[ele  for ele in category.split('.') if ele!='00']
    cate_len=len(category_list)
    if cate_len==1:
        category_path = "category_path1"
        column_category="category_path2"
    elif cate_len==2:
        category_path = "category_path2"
        column_category = "category_path3"
    else:
        category_path = "category_path3"
        column_category = "category_path3"

    where=''

    date_str = data['queryDate']
    date_str_list = date_str.split('-')
    date_str_list[0] = str(int(date_str_list[0]) - 1)
    date_str_2 = '-'.join(date_str_list)
    where += " date_str in ('" + date_str + "'," + "'" + date_str_2 + "')"

    where += " and " + category_path + "='" + category + "'"

    if data.__contains__('endTime') and data['endTime'] != '':
        hour_str = data['endTime'].split(':')[0]
        where += " and hour_str <" + "'" + hour_str + "'"

    source=data['source']
    if source!='0':
        where+=' and source='+"'"+source+"'"

    if uv:
        fromplatform=data['fromPlatform']
        if fromplatform not in ['']:
            temp = ','.join(["'" + str(ele) + "'" for ele in fromplatform.split(',')])
            where+=' and from_platform in ('+temp+")"

    else:
        platform=data['platform']
        if platform not in ['']:
            temp=','.join(["'"+str(ele)+"'" for ele in platform.split(',')])
            where+=' and platform in ('+temp+")"

    shop_type=str(data['mgtType'])
    if shop_type!='0':
        where+=" and shop_type= '"+shop_type+"'"

    if uv:
        bd_id_prod = get_bd_where({'bd_id': str(data['bizType'])})
        if len(bd_id_prod) > 0:
            bd_id_prod = bd_id_prod.replace('bd_id', 'bd_id_prod')
            where+=bd_id_prod
    else:
        if data['bizType'] != 0:
            where += " and bd_id ='" + str(data['bizType']) + "'"


    return where,column_category


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

sd_zf_report_map={
    #指标
    'sd_zf_amount':"SUM(case when data_type='{}'then bargin_price * order_quantity else null end) AS amount",   #金额
    'sd_zf_package':"COUNT(DISTINCT case when data_type='{}'then order_id else null end) AS package",     #包裹量
    'sd_zf_customer':"COUNT(DISTINCT case when data_type='{}'then cust_id else null end) AS customer",       #人数
    'sd_zf_new_customer':"COUNT(DISTINCT CASE WHEN data_type='{}' and new_id = '1' then cust_id else null end) AS new_customer" #新客
}

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

def gen_sqldata(sql,cursor,offset=1000):
    '''

    :param sql: msql 查询
    :param cursor:
    :param offset: 偏移量
    :return:
    '''
    initoffset=0
    sampletimes=0
    while sampletimes<100:
        newsql = sql + " limit " + str(initoffset) + ","+str(int(offset/2))
        cursor.execute(newsql)
        raw_data = cursor.fetchall()
        yield raw_data
        sampletimes+=1
        initoffset+=offset



#用户分析优化sql
#用户分支优化各指标计算逻辑
user_indicator_op_cal_dict={
    "new_uv":['uniqExactMerge(device_id_state) as new_uv','mdata_flows_user_realtime_day_all'],  #"新访UV"
    "new_uv_ratio":'new_uv/uv as new_uv_ratio',  #"新访uv占比"
    "register_number":['count(distinct cust_id) as register_number','mdata_customer_new_all'] ,    #"新增注册用户"
    "new_create_parent_uv_sd":['groupBitmapMerge(cust_id_state) as new_create_parent_uv_sd','dm_order_create_day'],  #"新增收订用户"
    "new_create_parent_uv_zf":['groupBitmapMerge(cust_id_state) as new_create_parent_uv_zf','dm_order_pay_day'],             #"新增支付用户"
    "new_create_parent_uv_ck": ['groupBitmapMerge(cust_id_state) as new_create_parent_uv_ck','dm_order_send_day'],             #"新增出库用户"
    "uv":['uniqExactMerge(device_id_state) as uv','mdata_flows_user_realtime_day_all'] ,        #"活跃UV"
    "create_parent_uv_sd":['groupBitmapMerge(cust_id_state) as create_parent_uv_sd','dm_order_create_day'],#"收订用户"
    "create_parent_uv_zf":['groupBitmapMerge(cust_id_state) as create_parent_uv_zf','dm_order_pay_day'],#'"支付用户"
    "create_parent_uv_ck":['groupBitmapMerge(cust_id_state) as create_parent_uv_ck','dm_order_send_day'],#"出库用户",
    "daycount_ratio_sd":['groupBitmapMerge(parent_id_state)/groupBitmapMerge(cust_id_state) as daycount_ratio_sd','dm_order_create_day'],#"收订下单频次",
    "daycount_ratio_zf":['groupBitmapMerge(parent_id_state)/groupBitmapMerge(cust_id_state) as daycount_ratio_zf','dm_order_pay_day'] #"支付下单频次"
}

def get_sql_for_user_analysis_overview_op(data,indicator):
    '''用户分析优化'''

    date_type=data['date_type']
    if date_type=='d':
        new_flag='day'
        column_date = 'date_str'
    elif date_type=='w':
        new_flag = 'week'
        column_date='toStartOfWeek(toDate(date_str), 1) as date_str'
    elif date_type=='m':
        new_flag = 'month'
        column_date='toStartOfMonth(toDate(date_str)) as date_str'
    else:
        new_flag='quarter'
        column_date = 'toStartOfQuarter(toDate(date_str)) as date_str'



    # where=" where bd_id IN (1,4,9,15,16) AND platform IN (1,2) and shop_type=1 and  date_str IN ('2021-05-12','2020-05-12','2021-05-05','2021-05-11') "
    groupby = "  group by date_str "
    orderby = " order by date_str desc"

    #根据指标拼接sql，一个
    sql_list=[]
    i=1
    outer_column=[]
    indicator_list=[]
    for ename,cname in indicator.items():

        if ename=='new_uv_ratio':
            continue

        #有几个指标条件需要特殊处理
        yinhao=False
        if ename in ['new_uv','register_number','uv']:
            yinhao=True
            bdid="bd_id != '6' and "
        else:
            bdid = "bd_id != 6 and "

        if ename in ['register_number']:
            yinhao = True
            bdid = ''

        where = ' where '+bdid

        where += get_platform_where(data, yinhao)
        where += get_bd_where(data,yinhao)
        where += get_shoptype_where(data,yinhao)
        if ename not in ['new_uv', 'register_number', 'uv']:
            where += get_eliminate_where(data)
        where += get_time_where(data)

        append_where = ''
        if ename=='new_uv':
            append_where=" and new_id=1 and type='prod' "

        if ename=='uv':
            append_where=" and type='prod' "

        if ename.startswith('new_create'):
            append_where = " and {}_new_flag=1 ".format(new_flag)


        newwhere = where + append_where

        alias_name="t"+str(i)
        outer_column.append(ename)

        column = user_indicator_op_cal_dict[ename][0]+" ,"+column_date
        table = 'bi_mdata.'+user_indicator_op_cal_dict[ename][1]

        sql="select " + column + " from " + table + " t "+newwhere + groupby + orderby
        sql_list.append(sql)

        indicator_list.append(ename)

        if ename=="new_uv":
            new_uv_sql=sql
        if ename=='uv':
            uv_sql = sql

    #新访UV占比
    uv_ratio_sql="select t1."+"new_uv / t2.uv*100 as new_uv_ratio,t1.date_str  from ("+new_uv_sql+") t1 "+" left join ("+uv_sql+") t2 on t1.date_str=t2.date_str "+orderby

    sql_list.append(uv_ratio_sql)
    indicator_list.append('new_uv_ratio')

    return sql_list,indicator_list

    #组装sql  ，此处有bug
    length=len(sql_list)
    sql_list=[sql_list[0]]+[sql_list[i] + " on t1" + ".date_str=t" + str(i + 1) + ".date_str" for i in range(1, length)]
    sql = ' left join '.join(sql_list)

    #最后算new_uv_ratio

    if indicator.__contains__('new_uv_ratio') :
        uv_index=outer_column.index('uv')+1
        new_uv_index=outer_column.index('new_uv') + 1

        new_uv_ration_column="t"+str(new_uv_index)+".new_uv" +" / "+"t"+str(uv_index)+".uv *100" +" as new_uv_ratio"

    outer_column=["t"+str(i+1)+"."+outer_column[i] for i in range(0,len(outer_column))]

    outer_column.insert(1,new_uv_ration_column)

    final_sql="select "+",".join(outer_column) +", t1.date_str from "+sql +orderby


    return final_sql