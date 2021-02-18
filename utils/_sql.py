from .date import get_tb_hb_date
from .tb_hb import tb_hb_cal,get_tb_hb_key

def get_platform_where(data,yinhao=False):
    '''获取platform对应的过滤条件'''

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
        sourcewhere = "("+_yin+'1'+_yin+','+_yin+'2'+_yin+','+_yin+'3'+_yin+","+_yin+'4'+_yin+")"
    else:
        sourcewhere = "(" + _yin+source+_yin + ")"
    #
    platformwhere = ''
    if source == '1' and parentplatform == '3':
        platformwhere = "("+_yin+'12'+_yin+','+_yin+'20'+_yin+")"
    elif source == '1' and parentplatform == '4':
        platformwhere = "("+_yin+'0'+_yin+")"
    elif source == '1' and parentplatform == '1' and platform == 'all':  #
        platformwhere = "("+_yin+'1'+_yin+','+_yin+'2'+_yin+")"
    elif source == '1' and parentplatform == '2' and platform == 'all':
        platformwhere = "("+_yin+'3'+_yin+','+_yin+'4'+_yin+','+_yin+'5'\
        +_yin +','+_yin+'6'+_yin+','+_yin+'7'+_yin+','+_yin+'8'+_yin+','+_yin+'9'+_yin+")"
    elif source=='1' and parentplatform=='all':
        platformwhere = "("+_yin+'0'+_yin+','+ _yin+'1'+_yin+','+_yin+'2'+_yin+ ','+_yin + '3' +\
                        _yin + ',' + _yin + '4' + _yin + ',' + _yin + '5' \
                        + _yin + ',' + _yin + '6' + _yin + ',' + _yin + '7' + _yin + ',' \
                        + _yin + '8' + _yin + ',' + _yin + '9' + _yin +','+ _yin+'12'+_yin+','+_yin+'20'+_yin+")"
    else:
        platformwhere = "(" +_yin+ platform +_yin+ ")"

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

    show=True                              #默认显示
    if source not in ['2', '3', '4']:                       # 天猫、抖音、拼多多下钻页没有平台分布
        if parentplatform not in ['2', '3', '4']:           # 轻应用\H5\PC没有平台分布

            if parentplatform != 'all':
                if platform =='all':          # 安卓、IOS没有平台分布
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


def get_bd_where(data):
    '''获取bd对应的过滤条件'''
    bd = data['bd_id']
    if bd == '1':                    #出版物
        bd_id = '(5,12)'
    elif bd == '2':                 #百货
        bd_id = '(1,4,9,15,16)'
    elif bd == '3':                 #数字
        bd_id = '(6)'
    elif bd == '4':                 #文创
        bd_id = '(20,21,23)'
    else:
        bd_id = 'all'
    bdwhere=''
    if bd_id !='all':
        bdwhere=" and bd_id in " +bd_id
    return bdwhere

def get_shoptype_where(data):
    '''获取shoptype对应的过滤条件'''
    shoptype= data['shop_type']

    if shoptype == '1':
        shop_type = '(1)'
    elif shoptype == '2':
        shop_type = '(2)'
    else:
        shop_type = 'all'

    shoptypewhere=''
    if shop_type!='all':
        shoptypewhere = " and shop_type in " + shop_type
    return shoptypewhere

def get_eliminate_where(data):
    '''获取shoptype对应的过滤条件'''
    jianggong= data['eliminate_type']

    if jianggong == '1':
        jianggong_type = '(1)'
    elif jianggong == '2':
        jianggong_type = '(1)'
    else:
        jianggong_type = 'all'

    jianggongwhere=''
    if jianggong_type!='all':
        jianggongwhere = " and is_jiangong in " + jianggong_type
    return jianggongwhere

'''返回sql筛选的时间（环比、同比）条件'''
def get_time_where(data):
    '''
    :param data: 字典
    :return:
    '''
    datetype=data['date_type']
    date=data['date_str']

    datewhere=''
    tb_hb_date = get_tb_hb_date(date, datetype)

    if len(tb_hb_date)==4:     #天返回环比、周同比、年同比

        today=tb_hb_date[0]
        yesterday=tb_hb_date[1]
        last_week_day=tb_hb_date[2]
        last_year_day=tb_hb_date[3]

        datewhere += ' and date_str in ' + "('" + today+"','"+yesterday+"','"+last_week_day+"','"+last_year_day+"')"

    else:      #周、月、季返回环比、同比
        hb_date=tb_hb_date[0]
        tb_date=tb_hb_date[1]

        for ele in tb_hb_date:
            if ele[0] is not None:
                 datewhere+= " date_str between '"+ele[0]+"' and '"+ele[1]+"' or "
        datewhere=" and ("+datewhere.strip('or ')+" )"
    return datewhere

'''下钻页各指标同比环比计算'''
def get_drill_tb_hb(ck_data,name_dict,date,datetype,misskeyshow=True,misskeyvalue='--'):
    '''
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
        if fromplatform not in ['','0']:
            temp = ','.join(["'" + str(ele) + "'" for ele in fromplatform.split(',')])
            where+=' and from_platform in ('+temp+")"

    else:
        platform=data['platform']
        if platform not in ['0','']:
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


def get_uv_sql_for_report(data,tablename):
    '''实时报表-uv'''
    where,column_category=get_where_for_report(data)
    if 'category_path2' not in where:
        uvwhere = where
    else:
        uvwhere=where+" and category != '"+data['categoryPath']+"'"+" and category !='' "
    column=column_category+" as category,"+"COUNT(DISTINCT device_id) AS UV,toString(date_str) as date_str"

    order_by=" order by category,date_str desc"

    sql=" select "+column+" from "+tablename+" where "+uvwhere+" group by category,date_str "+order_by+" limit 4"
    return sql

sd_zf_report_map={
    #指标
    'sd_zf_amount':"SUM(case when data_type='{}'then bargin_price * order_quantity else null end) AS amount",   #金额
    'sd_zf_package':"COUNT(DISTINCT case when data_type='{}'then order_id else null end) AS package",     #包裹量
    'sd_zf_customer':"COUNT(DISTINCT case when data_type='{}'then cust_id else null end) AS customer",       #人数
    'sd_zf_new_customer':"COUNT(DISTINCT CASE WHEN data_type='{}' and new_id = '1' then cust_id else null end) AS new_customer" #新客
}

def get_sd_info_sql_for_report(data,tablename):
    '''实时报表收订-其他指标'''
    where, column_category = get_where_for_report(data,False)

    where += " and data_type in ('1','4')"
    if 'category_path2' not in where:
        sdwhere=where
    else:
        sdwhere = where + " and category != '" + data['categoryPath'] + "'" + " and category !='' "

    sd_zf_category = column_category + " as category"

    column=sd_zf_category+","

    for cloumn_key in sd_zf_report_map.keys():
        column+=sd_zf_report_map[cloumn_key].format('1')+","

    sd_zf_order="COUNT(DISTINCT case when data_type = '1' then  parent_id else null end) AS order_sum"

    #收订
    sd_cancel = "COUNT(DISTINCT case when data_type='4' then parent_id else null end) AS order_cancel"
    sd_cancel_rate="(case when order_sum!=0 then order_cancel /order_sum *100 else null end ) as sd_cancel_rate"

    column+=sd_cancel_rate+",date_str,"+sd_zf_order+","+sd_cancel

    sd_sql="select "+column+ \
           " from "+tablename+ \
           " where "+sdwhere+" group by "+"category,date_str"+" order by category,date_str desc limit 4"
    return sd_sql


def get_zf_info_sql_for_report(data,tablename):
    '''实时报表支付-其他指标'''
    where, column_category = get_where_for_report(data,False)

    where += " and data_type in ('2','4')"
    if 'category_path2' not in where:
        zfwhere = where
    else:
        zfwhere = where + " and category != '" + data['categoryPath'] + "'" + " and category !='' "

    sd_zf_category = column_category + " as category"

    column = sd_zf_category + ","

    for cloumn_key in sd_zf_report_map.keys():
        column += sd_zf_report_map[cloumn_key].format('2') + ","

    sd_zf_order = "COUNT(DISTINCT case when data_type = '2' then  parent_id else null end) AS order_sum"

    # 支付
    column += "date_str," + sd_zf_order

    a_sql=" select "+column+" from "+tablename+" where "+zfwhere+ " group by " + "category,date_str"

    b_sql=" select COUNT(DISTINCT parent_id) as cancel_num,date_str,category from ("+ \
            "select parent_id,COUNT(DISTINCT data_type ) AS num,date_str,"+column_category+ " as category"+ \
            " from "+tablename+" where "+zfwhere+" group by parent_id,date_str,category having num>1) a"+ \
            " group by category,date_str"

    zf_sql = "select " +" category,amount,package,customer,new_customer, " + \
            "case when a.order_sum!=0 then b.cancel_num /a.order_sum* 100 else null end as zf_cancel_rate,date_str "+ \
             " from (" +a_sql  +") a left join ("+ \
             b_sql+") b"+" on a.category = b.category and a.date_str = b.date_str order by category,date_str desc limit 4 "

    return zf_sql