from utils.date import get_tb_hb_date,get_month_end_day
from utils.tb_hb import tb_hb_cal,get_tb_hb_key_list
from utils.util import is_number
from resources.map import user_drill_catgory_path_dict,source_dict,parent_platform_cat,bd_id_cat,bd_id_dict,shop_type_dict


'''获取platform对应的过滤条件'''
def get_platform_where(data,is_str=False,is_raw=False):
    '''
    @
    :param data:
    :param is_str:
    :param is_raw:
    :return:
    '''

    source = data['source']
    parentplatform = data['parent_platform']
    platform = data['platform']

    sourcedict=dict(source_dict)
    #
    if is_str:          #默认不加
        _yin="'"
    else:
        _yin=""

    source_where = ''
    platform_where=''
    source_data = []
    platform_data = []

    if source == 'all':
        sourcedict.pop('all')
        source_data = sourcedict.keys()
    else:
        source_data = [source]
        if parentplatform == 'all' and platform == 'all':
            pass
        elif parentplatform != 'all' and platform == 'all':
            if is_raw:
                platform_data = [parentplatform]
            else:
                platform_data = parent_platform_cat[parentplatform]
        else:
            platform_data = [platform]

    source_where += " source in (" + ','.join([_yin + ele + _yin for ele in source_data]) + ")"
    if platform_data != []:
        platform_where = " and platform in (" + ','.join([_yin + ele + _yin for ele in platform_data]) + ")"

    return source_where + platform_where



def get_platform_where_realtime(data,indicator):
    '''实时platform条件'''
    source = data['source']
    parentplatform = data['parent_platform']
    platform = data['platform']

    platwhere_uv=''
    platwhere=''
    if parentplatform == "all":
        if source !='all':
            platwhere=" source = '"+source+"'"

        else:
            platwhere = " source in ('1','2','3','4')"
        platwhere_uv = platwhere
    elif parentplatform == '1':
        if platform == 'all':
            platwhere_uv = "  from_platform in ('3','7','2') "
            platwhere="  platform in ('1','2') "
        elif platform == '1':
            platwhere_uv = "  from_platform in ('2') "
            platwhere = " platform in ('1') "
        elif platform == '2':
            platwhere_uv = " from_platform in ('3','7') "
            platwhere = " platform in ('2') "
    elif parentplatform == '2':

        if platform == '4':
            platform_uv = '21'
            platform_=platform
        elif platform == '3':
            platform_uv='26'
            platform_=platform
        elif platform == '5':
            platform_uv='23'
            platform_=platform
        elif platform=='all':
            platform_uv="26','21','23','6','7','8','9"
            platform_="3','4','5','6','7','8','9"
        else:
            platform_ = platform
            platform_uv =  platform

        platwhere_uv = " from_platform in ('"+platform_uv+"')"
        platwhere = " platform in ('"+platform_+"')"
    elif parentplatform == '3':
        platwhere_uv=" from_platform in ('12','20') "
        platwhere = " platform in ('12','20') "

    elif parentplatform == '4':
        platwhere_uv = " from_platform in ('0') "
        platwhere = " platform in ('0') "

    return platwhere_uv,platwhere


def is_platform_show(data):
    '''下钻页平台是否显示'''
    datacopy=dict(data)

    source = datacopy['source']
    parentplatform = datacopy['parent_platform']
    platform = datacopy['platform']

    show=True                                                   #默认显示
    if source in ['2', '3', '4']:                               # 天猫、抖音、拼多多下钻页没有平台分布
        show=False
    else:
        if parentplatform  in ['2', '3', '4']:                  # 轻应用\H5\PC没有平台分布
            show = False
        else:
            if parentplatform == '1' and platform !='all':     #安卓、IOS没有平台分布
                show = False
    return show

'''获取bd对应的过滤条件'''
def get_bd_where(data,is_str=False,is_raw=False):
    '''
    @ 下钻页平台分布 where 筛选条件
    :param data:
    :param is_raw: 直接返回bd、还是返回bd细分类cat
    :param is_str: 在sql中是否需要加引号，默认不加
    :return:
    '''

    bd = data['bd_id']
    bdid_dict = dict(bd_id_dict)
    bdid_cat = dict(bd_id_cat)

    bdwhere=" and bd_id in "
    bdwhere_data=[]

    if is_str:
        _yin = "'"
    else:
        _yin = ""

    if is_raw:              #取值bd
        if bd !='all':
            bdwhere_data=[bd]
        else:
            bdid_dict.pop('all')
            bdwhere_data=bdid_dict.keys()
    else:                  #使用bd 下细分 catgory
        if bd != 'all':
            bdwhere_data=bd_id_cat[bd]
            if bd == '5':
                bdwhere = " and bd_id not in "

    if bdwhere_data != []:
        bdwhere += "(" + ','.join([_yin + str(ele) + _yin for ele in bdwhere_data]) + ")"
    else:
        bdwhere=''

    return bdwhere


'''获取shoptype对应的过滤条件'''
def get_shoptype_where(data,is_str=False):
    '''

    :param data:
    :param yinhao:
    :return:
    '''

    shoptype= data['shop_type']
    shoptype_dict = dict(shop_type_dict)

    if is_str:  # 默认不加
        _yin = "'"
    else:
        _yin = ""

    shoptype_data = []
    if shoptype == 'all':
        # shoptype_dict.pop('all')
        # shoptype_data = shoptype_dict.keys()
        pass
    else:
        shoptype_data = [shoptype]

    if shoptype_data != []:
        shoptypewhere = " and shop_type in (" + ','.join([_yin + str(ele) + _yin for ele in shoptype_data]) + ")"
    else:
        shoptypewhere = ''

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

        datewhere += ' and date_str in ' + "('" + today+"','"+yesterday+"','"+last_week_day+"','"+last_year_day+"')"

        if datetype.startswith('h'):
            hour=date.split(' ')[1]
            datewhere+=" and hour_str <= '"+hour+"'"

    else:                   #周、月、季返回环比、同比去年

        for ele in tb_hb_date:
            if ele[0] is not None:
                 datewhere+= " t.date_str between '"+ele[0]+"' and '"+ele[1]+"' or "
        datewhere=" and ("+datewhere.strip('or ')+" )"
    return datewhere


#平台分布列
def get_plat_column(namedict,is_str):
    '''

    :param namedict: 平台字典
    :param is_str: sql中出现的platform代码数字是否需要加引号
    :return:
    '''

    yinhao=''
    if is_str:
        yinhao="'"

    column_plat=''
    for key, value in namedict.items():
        if value != []:
            column_plat += " when source ='1' and platform in " + "(" + ','.join(
                [yinhao + str(ele) + yinhao for ele in value]) + ")" + " then '" + key + "'"
    column_plat += " else '" + key + "' end"
    column_plat = "( case " + column_plat + " ) as _platform,"

    return column_plat


#事业部分布列
def get_bd_column(namedict,is_str):
    yinhao = ''
    if is_str:
        yinhao = "'"

    column_bd = '( case'
    namedictcopy = dict(namedict)
    if len(namedict) == 13:  #事业部细分

        namedictcopy.pop('13')

        for key in namedictcopy.keys():
            column_bd += " when category_path3 in " + str(user_drill_catgory_path_dict[key]) + " then '" + key + "'"
        # others
        column_bd += " else '13' end as _bd_id,"

    else:
        namedictcopy.pop('all')
        for key, value in namedictcopy.items():
            if key!='5':
                column_bd += " when bd_id in " + "(" + ','.join(
                    [yinhao + str(ele) + yinhao for ele in bd_id_cat[key]]) + ")" + " then '" + key + "'"

        column_bd += " else '5' end ) as _bd_id,"

    return column_bd


#用户分析是否显示计算下钻项
def is_show_for_user_drill(indicator,item):

    if item =='bd':
        if indicator not in ['new_uv','uv','new_uv_ratio','register_number']:
            return True

    if item == 'customer':
        if indicator in ['uv','create_parent_uv_sd','create_parent_uv_zf','create_parent_uv_ck','daycount_ratio_sd','daycount_ratio_zf']:
            return True

    if item == 'quantile':
        if indicator in ['daycount_ratio_sd','daycount_ratio_zf']:
            return True

    return False


#数据合并计算
def data_merge(ck_data):
    '''
    根据时间日期、首位分类进行数据合并
    :param ck_data: 细分的数据，加和 合并，如[['1','3',100,'2021-06-06'],['1','20',120,'2021-06-06'],
            ['1','4',100,'2021-06-06'],['1','3',100,'2021-06-05'],['1','20',100,'2021-06-05'],
            ['2','5',110,'2021-06-06'],['2','6',80,'2021-06-06'],['2','5',200,'2021-06-05']]
    :return:[['1',320,'2021-06-06'],['1',200,'2021-06-05'],['2',190,'2021-06-06'],['2',200,'2021-06-05']]
    '''

    # 数据预处理
    ckdata = [[ele[0], float(ele[-2]), ele[-1]] for ele in ck_data if is_number(ele[-2]) and ele[1] != '0']
    # 依据时间排序、分类排序
    ckdata = sorted(ckdata, key=lambda s: s[2], reverse=True)
    ckdata = sorted(ckdata, key=lambda s: s[0])

    length=len(ckdata)
    new_ckdata=[]

    cmp=[None,0,None]

    for ele in ckdata:

        if ele[0] == cmp[0] and ele[-1] == cmp[-1]:
            new_ckdata[-1][1] = new_ckdata[-1][1] + ele[1]

        else:
            new_ckdata.append(ele)
            cmp = ele

    return new_ckdata



#模块首页同环比计算
def get_overview_tb_hb(ck_data,tbhb_keydict,date,datetype,misskeyshow=True,misskeyvalue='--'):
    '''

    :param ck_data: 二维列表，
    :param cname: 指标名称
    :param date:
    :param datetype:
    :param misskeyshow:
    :param misskeyvalue:
    :return:
    '''

    #数据预处理
    ck_data=[[float(ele[0]), ele[1]] for ele in ck_data if is_number(ele[0]) and ele[0] != '0']
    #依据时间排序
    ck_data=sorted(ck_data, key=lambda s: s[1], reverse=True)

    #实际可以计算的同环比键
    tb_hb_key_list = get_tb_hb_key_list(ck_data, date, datetype,tbhb_keydict)

    valuedict = {}

    if len(tb_hb_key_list) == 0 or ck_data == []:
        # tempdict[name]={'value':'--'}
        valuedict ={}            #该指标没有值，不能计算同环比

    else :                       # 可以进行同环比计算
        newdata = tb_hb_cal(ck_data, tb_hb_key_list)

        for key,value in tbhb_keydict.items():
            if newdata.__contains__(key):
                valuedict[value]=newdata[key]
            else:
                if misskeyshow:         # 显示缺失值
                    valuedict[value] = misskeyvalue

    return valuedict



def get_drill_tb_hb(ck_data,tbhb_keydict,name_dict,date,datetype,misskeyshow=True,misskeyvalue='--'):
    '''
                            下钻页各指标同比环比计算
    :param ck_data: 二维列表,第一列作为下钻分类，如[[1,100,'2021-05-12'],[1,120,'2021-05-11'],[1,90,'2021-05-05'],[1,80,'2020-05-12'],
                                                    [2,120,'2021-05-12'],[2,100,'2021-05-11'],[2,110,'2021-05-05']]
    :param name_dict: 字典
    :param date: 时间字符串
    :param datetype: 时间类型
    :return: 同环比
    '''
    sqldata={}
    tempdict={}
    temp = []
    i = 0
    ckdata = []
    if ck_data == []:
        return sqldata
    else:
        # 数据预处理
        ckdata = [[ele[0],float(ele[1]),ele[2]] for ele in ck_data if is_number(ele[1]) and ele[1] != '0']
        # 依据时间排序、分类排序
        ckdata = sorted(ckdata, key=lambda s: s[2], reverse=True)
        ckdata = sorted(ckdata, key=lambda s: s[0])

    begin_index = ckdata[i][0]

    is_need_cal = False
    length = len(ckdata)
    while i <= length - 1:

        if begin_index == ckdata[i][0]:
            temp.append(ckdata[i][1:])  # 取值和时间
            keystr = str(begin_index)
            i += 1
        else:
            begin_index = ckdata[i][0]
            is_need_cal = True

        if i == length:
            is_need_cal = True

        if is_need_cal:
            # 计算环比、同比
            tb_hb_key_list= get_tb_hb_key_list(temp, date, datetype,tbhb_keydict)

            if len(tb_hb_key_list) > 0:  # 可以进行同环比计算
                newdata = tb_hb_cal(temp,tb_hb_key_list)


                #没有的同环比key，值是否设置为默认值'--'
                for key, value in tbhb_keydict.items():
                    if newdata.__contains__(key):
                        tempdict[value] = newdata[key]
                    else:
                        if misskeyshow:  # 显示缺失值
                            tempdict[value] = misskeyvalue

                sqldata[name_dict[keystr]] = tempdict


            #
            temp = []
            tempdict={}
            is_need_cal = False

    if misskeyshow:
        for key,value in name_dict.items():
            if not sqldata.__contains__(value):
                sqldata[value] = {}        # 赋值为空

    return sqldata


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
def get_where_for_analysis_overview_op(data,indicator,overview=True):
    '''

    :param data:
    :param indicator: 指标的英文名称 ename
    :param overview: 默认首页时间，否则下钻页时间条件
    :return:
    '''
    date_type = data['date_type']
    if date_type == 'd':
        new_flag = 'day'
    elif date_type == 'w':
        new_flag = 'week'
    elif date_type == 'm':
        new_flag = 'month'
    else:
        new_flag = 'quarter'

    #指标名称
    ename=indicator

    where = " where "
    platyinhao=False
    is_raw = False

    #plat
    if date_type.startswith('h'):
        tempplat = get_platform_where_realtime(data, indicator)
        if ename in ['new_uv', 'uv', 'new_uv_ratio']:
            plat = tempplat[0]
        else:
            plat = tempplat[1]
        where += plat
    else:
        if ename in ['new_uv', 'register_number', 'uv', 'new_uv_ratio']:
            platyinhao = True
        where += get_platform_where(data, platyinhao,is_raw)

    #bd
    bdid=''
    bd_id_columnname = 'bd_id'
    bdyinhao = False
    is_raw = False

    if date_type != 'h':
        if ename in ['new_uv', 'register_number', 'uv','new_uv_ratio']:
            bdyinhao = True
            bdid = " and bd_id != '6'"
        else:
            bdid = " and bd_id != 6"

    if ename in ['register_number']:
        bdyinhao = True
        bdid = ''

    where += bdid

    if date_type.startswith('h'):
        if ename in ['uv', 'new_uv', 'new_uv_ratio']:
            is_raw = False
            bdyinhao = False
            bd_id_columnname = 'bd_id_prod'
        else:
            is_raw = True
            bdyinhao = True
    else:
        if ename in ['uv', 'new_uv', 'new_uv_ratio']:
            bdyinhao = True

    bdwhere = get_bd_where(data, bdyinhao, is_raw)
    where += bdwhere.replace('bd_id', bd_id_columnname)


    #shoptype
    shopyinhao = False

    if date_type.startswith('h'):

        shopyinhao = True
        if ename in ['uv', 'new_uv', 'new_uv_ratio']:
            if data['shop_type'] == '1':
                where +=" and shop_type IN ('0','1')"
            else:
                where += " and shop_type IN ('2')"
        else:
            where += get_shoptype_where(data, shopyinhao)

    else:
        if ename in ['uv', 'new_uv', 'new_uv_ratio']:
            shopyinhao = True

        where += get_shoptype_where(data, shopyinhao)


    #eliminate
    if ename not in ['new_uv', 'register_number', 'uv','new_uv_ratio'] and date_type !='h':
        where += get_eliminate_where(data)

    where += get_time_where(data)


    #其他附加筛选条件
    append_where = ''

    if ename == 'new_uv' and date_type !='h':
        append_where = " and new_id=1"
        if data['bd_id'] !='all':
            append_where += " and type='prod' "

    if ename == 'uv' and date_type !='h' and data['bd_id'] !='all':
        append_where = " and type='prod' "

    if ename.startswith('new_create') and date_type !='h':
        append_where = " and {}_new_flag=1 ".format(new_flag)

    if ename == "register_number":
        where = where.replace('platform', 'from_platform')

    newwhere = where + append_where

    return newwhere


