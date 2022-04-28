'''
#实时搜索词-接口校验
'''
import os
import sys,json
import datetime,re
import random,copy
import pandas as pd
import numpy as np
from db.map.search_word_map import *
from itertools import combinations,chain
from utils.util import diff
from utils.apihandle import ApiHandle
from utils.log import set_logger
from utils.util import dict_format
from utils.db import PyCK
from utils.date import get_lastdate_d_w_m_q

ck_table = "bi_mdata.realtime_search_word_all"
ck_path_table = "iocTest1.realtime_search_word_bd2cat_all"


#logger
real_search_logger = set_logger(logclass='file',filename='real_search')

#异动筛选条件中只能有一个同环比、但可以有多个值
def get_abnomal_filter():
    filter = {'relation': "", 'field': "", 'type': "", 'symbol': '', 'value': ""}
    relationlist = ['and' ,'or']
    fieldlist = list(abnomaly_field.keys())
    typelist = list(tbhb2name.keys())
    symbollist = list(ops.keys())
    valuelist = [10,20,400]
    filter_list = []
    for relation in relationlist:
        filter['relation'] = relation
        for field in fieldlist:
            filter['field'] = field
            for type in typelist:
                if type.endswith('lastyear'):
                    continue
                filter['type'] = type
                for symbol in symbollist:
                    filter['symbol'] = symbol
                    filter['value'] = str(valuelist[random.randint(0,len(valuelist)-1)])
                    #剔除不合法的条件
                    if field in ['scust_rate','sclick_rate']:
                        if type.startswith('hb'):  #没有同环比
                            continue
                        else:
                            filter['value'] = str(random.randint(0, 100)) + "%"
                    if type.startswith('hb'):
                        filter['value'] = str(random.randint(0,100))+"%"
                    filter_list.append(dict(filter))
    temp = [list(combinations(filter_list, i)) for i in range(1, 4)]
    # result = [list(ele) for ele in chain.from_iterable(temp)]
    result = []
    for ele in chain.from_iterable(temp):
        i=0
        for ele_item in ele:
            if ele_item['type'].startswith('hb'):
                i+=1
        if i > 1:
           continue
        result.append(list(ele))
    # for ele in result:
    #     ele[0]['relation'] = ''
    return result



def get_sql_data(data,date_dict,date_show,ciyun=False,wordline=False,):
    '''
    查数据库
    :param data:
    :return:
    '''
    ck_db = PyCK()
    tempdata = dict(data)

    searchword = tempdata['searchword']
    if searchword =='' :
        searchword_where = "1=1"
    else:
        searchword_where = "search_word='{searchword}'".format(searchword = searchword)

    bd_name = bd_id2name[tempdata['bdId']]

    inner_join_pathname_sql = ''
    if bd_name !='全部':
        inner_join_pathname_sql = "SELECT DISTINCT path_name FROM " \
                              "{} WHERE bd_name = '{}' AND path_name != ''".format(ck_path_table,bd_name)

    date_str = date_dict['value']
    sql_date_list_str = str(tuple(date_dict.values()))

    where = " where "

    if not wordline:
        hour_str = "'{hour_str}'"
        hour_str_value = str(int(tempdata['endTime'].split(":")[0]))
        if len(hour_str_value) == 2:
            word_select_where = word_select_time['10']
        else:
            word_select_where = word_select_time[hour_str_value]

        where += "search_pv "+word_select_where+" and main_product_id <> -1 and "

    else:
        hour_str = "{hour_str}"
        hour_str_value = "(SELECT toString(max(toUInt32(hour_str))) FROM bi_mdata.realtime_search_word_all where date_str ='{date_str}')"

    where +=" {searchword_where} and date_str in {sql_date_list_str} and hour_str = "+hour_str
    where_format = where.format(searchword_where= searchword_where,sql_date_list_str=sql_date_list_str,hour_str = hour_str_value)


    select_column = ','.join([key for key in sql_table.keys()])

    select_column += ',date_str '
    groupby = ""
    orderby = ""

    select_column_list = [''.join([key.capitalize() for key in key.split('_')]) for key in sql_table.keys()]
    item_key_list = [a[0].lower() + a[1:] for a in select_column_list] + ['date_str']

    if ciyun:
        orderby_item = data['orderBy']
        click_search_lower = orderby_item[:-2].lower()
        uv_pv_lower = orderby_item[-2:].lower()

        orderby_item_sql = click_search_lower+"_"+uv_pv_lower
        orderby = " order by " + orderby_item_sql +" desc "
        if data['anomaly'] == '':
            orderby+=" limit 50"

    if wordline :
        item_key_list = [key for key in sql_table.keys()]
        select_column = ','.join(item_key_list)
        item_key_list.append('date_str')

        if tempdata['anomaly'] != '1':

            if tempdata['searchword'] == '':
                item_key_list = ['search_uv','click_uv','search_pv','click_pv','create_cust_num']
                select_column =','.join(["sum("+ele+")" for ele in item_key_list])
                item_key_list.append('date_str')
                groupby = " group by date_str "

    sql = "select "+select_column+" from "+ck_table+where_format.format(date_str = date_str)+groupby+orderby
    if inner_join_pathname_sql !='':
        sql = "select a.* from ("+sql+") a inner join ("+inner_join_pathname_sql+") b on a.path2_name = b.path_name"

    sql_result = ck_db.get_result_from_db(sql)
    ck_db.close_db()
    #转为df
    df = pd.DataFrame(sql_result,columns = item_key_list)

    return df


def get_comparetype_list():
    '''
    同比环比组合
    :return:
    '''
    typelist= []
    for key,value in comparetype.items():
        typelist.append(key)
    result = [list(combinations(typelist, i)) for i in range(0, 4)]
    return [str(list(ele)) for ele in chain.from_iterable(result)]

def get_all_filters(date,starttime,endtime):

    all_filtes_list = []

    searchword_list = ['','辛亥革命','诺贝尔文学奖作品','乡土中国'][0:1]
    querydate = date
    compretype_list = get_comparetype_list()
    abnomal_filter_list = get_abnomal_filter()[0:100]

    for searchword in searchword_list:
        for anomal_flag in anomaly.keys():
            for comparetype in compretype_list[7:]:
                for bd_id in bd_id2name.keys():
                    for abnomal_filter in abnomal_filter_list:
                        if anomal_flag == '':
                            anomaly_value = [
                                {'relation': "", 'field': "spv", 'type': "hb", 'symbol': ">=", 'value': "50%"}]
                        else:
                            temp = copy.deepcopy(abnomal_filter)
                            temp[0]['relation'] = ''
                            anomal_flag = temp
                            anomaly_value = temp

                        all_filtes_list.append(
                            {
                                'searchword':searchword,
                                'anomaly':anomal_flag,
                                'anomalyValue':anomaly_value,
                                'compareType': comparetype,
                                'queryDate': date,
                                'startTime': starttime,
                                'endTime': endtime,
                                'pageNo':1,
                                'pageSize':10,
                                'sort':'',
                                'orderBy':'',
                                'bdId':bd_id
                            }
                        )

    return all_filtes_list



def abnomal_check(abnomaly_value,sqlresult):

    is_abnomal = True
    for ele in abnomaly_value:
        zhibiao = ele['field']
        tbhbtype = ele['type']
        oper = ele['symbol']
        relation = ele['relation']

        zhibiao_value = round(float(ele['value'].strip('%')),2)

        zhibiao_name = abnomaly_field[zhibiao]
        if tbhbtype !='number':
            zhibiao_name += tbhb2name[tbhbtype]
        item_value = sqlresult[zhibiao_name]

        temp = False
        if item_value !='-' and item_value is not None:
            temp = ops[oper](item_value,zhibiao_value)

        if relation != 'or':
            is_abnomal = is_abnomal and temp
        else:
            is_abnomal = is_abnomal or temp

    return is_abnomal
    pass

def do_job(date,starttime,endtime,token):
    url ="http://test.newwk.dangdang.com/api/v3/reportForm/realtime/searchword"
    #遍历前端筛选条件
    filter_list = get_all_filters(date,starttime,endtime)

    len1 = len(filter_list)
    print(len1)

    #有同环比的指标
    key_tbhb = set()
    tbhb_dict = {}
    for key in main_table.keys():
        if key.endswith('WoW'):
            key_tbhb.add(key[:-3])
        elif key.endswith('YoY'):
            key_tbhb.add(key[:-3])
        elif key.endswith('LinkRelative'):
            key_tbhb.add(key[:-12])
        else:
            continue
        tbhb_dict[key] = '-'    #赋初值


    # 环比、同比上周、同比去年
    date_str = date
    date_dict = {
        'value': date_str,
        'LinkRelative': get_lastdate_d_w_m_q(date_str,'d').strftime('%Y-%m-%d'),
        'WoW': get_lastdate_d_w_m_q(date_str, 'w').strftime('%Y-%m-%d'),
        'YoY': get_lastdate_d_w_m_q(date_str,'y')
    }

    api_requst = ApiHandle(token)
    for filter in filter_list[0:]:
        #获取api结果
        data = dict(filter)

        #debug
#         data ={'searchword': '意林作文素材初中版', 'anomaly': [{'relation': '', 'field': 'spv', 'type': 'hb_lastweek', 'symbol': '>=', 'value': '31%'},
#                                                       {'relation': 'or', 'field': 'suv', 'type': 'hb', 'symbol': '>=', 'value': '99%'}], 'anomalyValue': [{'relation': '', 'field': 'spv', 'type': 'hb_lastweek', 'symbol': '>=', 'value': '31%'}, {'relation'
# : 'or', 'field': 'suv', 'type': 'hb', 'symbol': '>=', 'value': '99%'}], 'compareType': '[1, 2, 3]', 'queryDate': '2021-11-22', 'startTime': '00:00', 'endTime': '13:00', 'pageNo'
# : 1, 'pageSize': 10, 'sort': '', 'orderBy': '', 'bdId': '0'}
#
#         api_data_list, api_data_total_num = get_api_data(url, data, s)

        compartype_list = eval(data['compareType'])
        tbhb_date_show = {comparetype[i] for i in compartype_list}

        hour_str = data['endTime'].split(":")[0]

        #提取异动条件
        abnomal_date_show = set()
        abnomaly_value = []
        if data['anomaly'] != '':
            abnomaly_value = data['anomalyValue']
            abnomal_date_show = {tbhb2name[ele['type']] for ele in abnomaly_value}
        date_show_name = tbhb_date_show | abnomal_date_show
        date_show = [date_dict[ele] for ele in date_show_name]


        df = get_sql_data(data,date_dict,date_show)
        # test 所有搜索词
        search_word_all = list(set(df['searchWord']))

        i = 0
        test_total_num = len(search_word_all)

        # 每个条件最多抽取5个进行校验
        choice_words = random.sample(search_word_all, min(5, test_total_num))
        for word in choice_words:

            tbhb_dict_copy = dict(tbhb_dict)

            sqlresult_match = df[df['searchWord'] == word].copy()


            if date_str in sqlresult_match['date_str'].values:  # 可以进行比率、同环比计算
                sqlresult_match.sort_values('date_str', axis=0, ascending=False, inplace=True)
                sqlresult_match = sqlresult_match.apply(pd.to_numeric, errors='ignore')

                sqlresult_match['searchClickRate'] = round(
                    sqlresult_match['clickUv'] / sqlresult_match['searchUv'],2)
                sqlresult_match['searchCustRate'] = round(
                    sqlresult_match['createCustNum'] / sqlresult_match['searchUv'], 2)
                sqlresult_match['searchAmtRate'] = round(
                    sqlresult_match['createSaleAmt'] / sqlresult_match['searchUv'], 2)
                sqlresult_match['rpm'] = round(
                    sqlresult_match['createSaleAmt'] / sqlresult_match['searchPv'] * 1000, 2)

                sqlresult_match.replace(np.inf, np.nan, inplace=True)
                # sqlresult_match = sqlresult_match.where(sqlresult_match.notnull(), None)

                # 同环比
                tbhb_key_list = list(key_tbhb)
                tbhb_date_list = sqlresult_match['date_str'].to_list()
                tbhb_df = sqlresult_match[key_tbhb]

                base_date = datetime.strptime(tbhb_date_list[0], '%Y-%m-%d')

                for i in range(1, len(tbhb_date_list)):
                    tbhb_date_date = datetime.strptime(tbhb_date_list[i], '%Y-%m-%d')

                    diffdate = base_date - tbhb_date_date

                    temp_df = round((tbhb_df.iloc[0] / tbhb_df.iloc[i] - 1) * 100, 2)
                    temp_df.replace(np.inf, np.nan, inplace=True)
                    temp_df = temp_df.where(temp_df.notnull(), '-')
                    if diffdate.days == 1:
                        rwy = [ele + "LinkRelative" for ele in tbhb_key_list]

                    elif diffdate.days == 7:
                        rwy = [ele + "WoW" for ele in tbhb_key_list]

                    else:
                        rwy = [ele + "YoY" for ele in tbhb_key_list]

                    tbhb_dict_copy.update(dict(zip(rwy, temp_df)))
                # 合并值
                sqlresult = sqlresult_match.iloc[0].to_dict()
                sqlresult = dict_format(sqlresult)
                sqlresult.update(tbhb_dict_copy)
                sqlresult['hour_str'] = hour_str

                # 异动判别
                if data['anomaly'] != '':
                    is_abnomal = abnomal_check(abnomaly_value, sqlresult)

                    if not is_abnomal:
                        sqlresult = {}

                #
                data['searchword'] = word

                req = api_requst.post(url= url,data = data)
                api_result = json.loads(req.content.decode('utf-8'))
                api_data_list = api_result['payload']['modelList']  # 无返回数据 =[]
                total_num = int(api_result['payload']['total'])

                length_api_data = len(api_data_list)
                # 检查test-bug
                if sqlresult == {} and length_api_data != 0:
                    real_search_logger.info('数据库无,但api有此类条件数据，' + " 筛选条件:" + str(data) + "\n")
                    continue

                #api 数据处理
                apiresult = {}
                if length_api_data >0:
                    item = api_data_list[0]
                    keyset = set(main_table) & set(item)

                    api_item = {key: value for key, value in item.items() if key in keyset}

                    apiresult = {key: '-' if data['compareType'] == '[]' and key.endswith('Relative') else value for
                                 key, value in api_item.items()}
                    apiresult = dict_format(apiresult)
                    apiresult['date_str'] = date_str
                    apiresult['hour_str'] = hour_str

                #sql 同环比设置
                tbhb_date_hidden = list(set(comparetype.values()) - tbhb_date_show)
                pt = "|".join(tbhb_date_hidden)
                if pt !='':
                    sqlresult= {key: '-' if re.search(pt, key) else value for key, value in sqlresult.items()}

                # diff
                print(data)
                message = '-Success-'
                diffvalue = ''
                diff_value = diff(sqlresult, apiresult)

                if diff_value != {}:
                    message = '-Fail-'
                    diffvalue = diff_value

                    real_search_logger.info(message + " 筛选条件:" + str(data))
                    real_search_logger.info(diffvalue)
                    real_search_logger.info('\n')
    pass



def get_sql_ciyun_data(data):
    '''

    :param data: 筛选条件
    :return:
    '''
    date_str = data['queryDate']
    hour_str = data['endTime'].split(":")[0]


    date_dict = {
        'value': date_str,
        'LinkRelative': get_lastdate_d_w_m_q(date_str, 'd').strftime('%Y-%m-%d'),
        'WoW': get_lastdate_d_w_m_q(date_str, 'w').strftime('%Y-%m-%d'),
        'YoY': get_lastdate_d_w_m_q(date_str,'y')
    }

    tempdata = dict(data)

    searchword = tempdata['searchword']
    if searchword == '':
        searchword_where = "1=1"
    else:
        searchword_where = "search_word='{searchword}'".format(searchword=searchword)

    bd_name = bd_id2name[tempdata['bdId']]

    #bd
    inner_join_pathname_sql = ''
    if bd_name != '全部':
        inner_join_pathname_sql = "SELECT DISTINCT path_name FROM " \
                                  "{} WHERE bd_name = '{}' AND path_name != ''".format(ck_path_table, bd_name)

    order_column = data['orderBy']
    #筛选条件
    #where 条件
    hour_str_value = str(int(hour_str))
    if len(hour_str_value) == 2:
        word_select_where = word_select_time['10']
    else:
        word_select_where = word_select_time[hour_str_value]

    where = "search_pv " + word_select_where + " and main_product_id <> -1 and "
    where += " {searchword_where}".format(searchword_where = searchword_where)+" and date_str = '{date_str}' and hour_str = '" + hour_str+"'"

    #提取异动条件、字段
    yidong_column_list = []
    tbhb_type_list = ['number']

    column_tbhbtype_value = tuple()
    yidong_where = ''
    if data['anomaly'] != '':
        abnomaly_value = data['anomalyValue']
        for ele in abnomaly_value:
            field = ele['field']
            zhibiao = abnomaly_field[field]

            tbhbtype = ele['type']
            oper = ele['symbol']
            relation = ele['relation']
            value = ele['value']

            #存在需要计算同环比的指标(只允许有一个）

            zhibiao_where_name = 't0.'+zhibiao
            if tbhbtype.startswith('hb'):
                tbhb_type_list.append(tbhbtype)
                column_tbhbtype_value = (zhibiao, tbhbtype, oper, relation)
                zhibiao_where_name = zhibiao + tbhb2name[tbhbtype]
                # yidong_column_list.append(zhibiao_where_name)

            if zhibiao not in yidong_column_list:
                yidong_column_list.append(zhibiao)

            #异动where 条件
            yidong_where += relation+" "+zhibiao_where_name + oper+str(value).strip('%')+" "

    # 构造sql
    # 主字段
    main_column_list = list(sql_table.keys())
    main_column_str = ",".join(main_column_list)

    # order_column = abnomaly_field_to_maintable[order_column[:-2] + "_" + order_column[-2:].lower()]
    if order_column not in yidong_column_list:
        yidong_column_list.append(order_column)

    outer_column = ['search_word','path2_name'] + yidong_column_list

    #同环比字段添加到最后
    tbhb_column = []
    if column_tbhbtype_value !=():
        tbhb_column = [column_tbhbtype_value[0]+tbhb2name[column_tbhbtype_value[1]]]
    df_column = list(outer_column) + tbhb_column

    i=0
    for ele in outer_column:
        if abnomaly_field_cal_map.__contains__(ele):
             outer_column [i] = abnomaly_field_cal_map[ele]
        i+=1
    outer_column_str = ','.join(outer_column)

    sql_list = []
    i = 0
    for type in tbhb_type_list:

        temp_sql = "select " + main_column_str + " from " + ck_table +" where "+ where.format(date_str=date_dict[tbhb2name[type]])

        temp_sql = "select {outer_column_str} ".format(outer_column_str=outer_column_str)+" from (" + temp_sql + ") a "
        if inner_join_pathname_sql != '':
            temp_sql += " inner join (" + inner_join_pathname_sql + ") b on a.path2_name = b.path_name "

        temp_sql = "("+ temp_sql+") t"+str(i)
        sql_list.append(temp_sql)
        i += 1
        pass

    #最外层column
    hb_column =''
    if len(sql_list) > 1:
        sql = " left join ".join(sql_list) + " on t0.search_word = t1.search_word "
        hb_column = column_tbhbtype_value[0]
        hb_column_alias = tbhb_column[0]

        hb_column = ", case when t1.{hb_column} <>0 then round((t0.{hb_column} - t1.{hb_column}) / t1.{hb_column} *100,2) " \
                      "else null end as ".format(hb_column=hb_column) + hb_column_alias
    else:
        sql = sql_list[0]
    final_column = "t0.*"+hb_column

    orderby =" order by t0."+order_column+" desc "

    sql = "select "+final_column+ " from "+ sql + " where "+yidong_where+orderby+" limit 50"

    sql_result = ck_connect_new().execute(sql)
    df = pd.DataFrame(sql_result,columns = df_column)
    #
    searchword_pd = df['search_word']
    ordercolumn_pd = df[order_column]

    return dict(zip(searchword_pd,ordercolumn_pd))


def get_sql_wordline_data(data):
    '''

    :param data:
    :return:
    '''
    date_str = data['queryDate']
    hour_str = data['endTime'].split(":")[0]

    date_dict = {
        'value': date_str,
        'LinkRelative': get_lastdate_d_w_m_q(date_str, 'd',1).strftime('%Y-%m-%d'),
        'WoW': get_lastdate_d_w_m_q(date_str, 'w').strftime('%Y-%m-%d'),
        'YoY': get_lastdate_d_w_m_q(date_str,'y')
    }

    tempdata = dict(data)

    searchword = tempdata['searchword']
    if searchword == '':
        searchword_where = " 1=1"
    else:
        searchword_where = " search_word='{searchword}'".format(searchword=searchword)

    bd_name = bd_id2name[tempdata['bdId']]
    # bd
    inner_join_pathname_sql = ''
    if bd_name != '全部':
        inner_join_pathname_sql = "SELECT DISTINCT path_name FROM " \
                                  "{} WHERE bd_name = '{}' AND path_name != ''".format(ck_path_table, bd_name)

    # 筛选条件
    # where 条件
    where = " "

    # 提取异动条件、字段
    yidong_column_list = []
    tbhb_type_list = ['number']

    column_tbhbtype_value = tuple()
    yidong_where = ''
    if data['anomaly'] != '':
        abnomaly_value = data['anomalyValue']
        for ele in abnomaly_value:
            field = ele['field']
            zhibiao = abnomaly_field[field]

            tbhbtype = ele['type']
            oper = ele['symbol']
            relation = ele['relation']
            value = ele['value']

            # 存在需要计算同环比的指标(只允许有一个）
            value_format = value

            zhibiao_where_name = 't0.' + zhibiao
            if tbhbtype.startswith('hb'):
                tbhb_type_list.append(tbhbtype)
                column_tbhbtype_value = (zhibiao, tbhbtype, oper, relation)
                zhibiao_where_name = zhibiao + tbhb2name[tbhbtype]
                # yidong_column_list.append(zhibiao_where_name)
                value_format = value.strip('%')
            else:
                if value.endswith("%"):
                    value_format = str(float(value.strip('%')) / 100)

            if zhibiao not in yidong_column_list:
                yidong_column_list.append(zhibiao)

            # 异动where 条件
            yidong_where += relation + " " + zhibiao_where_name + oper + value_format + " "

    #主字段
    #构造sql
    main_column_list = list(sql_table.keys())
    main_column_str = ",".join(main_column_list)

    # order_column = abnomaly_field_to_maintable[order_column[:-2] + "_" + order_column[-2:].lower()]
    findby_column = ['search_pv', 'search_uv', 'click_pv', 'click_uv','createCustNum']

    findy_column_upper = [ele.split('_')[0]+ele.split('_')[1].capitalize() for ele in findby_column[:-1]]+[findby_column[-1]]
    outer_column = ['search_word', 'path2_name'] + list(set(findy_column_upper) | set(yidong_column_list))

    # 同环比字段添加到最后
    tbhb_column = []
    if column_tbhbtype_value != ():
        tbhb_column = [column_tbhbtype_value[0] + tbhb2name[column_tbhbtype_value[1]]]
    df_column = list(findby_column)

    i = 0
    for ele in outer_column:
        if abnomaly_field_cal_map.__contains__(ele):
            outer_column[i] = abnomaly_field_cal_map[ele]
        i += 1
    outer_column_str = ','.join(outer_column)

    sql_list = []
    i = 0
    hour_str = "(SELECT toString(max(toUInt32(hour_str))) FROM bi_mdata.realtime_search_word_all where date_str ='{date_str}')"
    where += " {searchword_where}".format(searchword_where=searchword_where)+ " and date_str = '{date_str}' and hour_str ={hour_str}"

    for type in tbhb_type_list:

        hour_str_format = hour_str.format(date_str = date_dict[tbhb2name[type]])

        temp_sql = "select " + main_column_str + " from " + ck_table + " where " + where.format(
            date_str=date_dict[tbhb2name[type]],hour_str = hour_str_format)

        temp_sql = "select {outer_column_str} ".format(
            outer_column_str=outer_column_str) + " from (" + temp_sql + ") a "
        if inner_join_pathname_sql != '':
            temp_sql += " inner join (" + inner_join_pathname_sql + ") b on a.path2_name = b.path_name "

        temp_sql = "(" + temp_sql + ") t" + str(i)
        sql_list.append(temp_sql)
        i += 1
        pass

    # 最外层column
    hb_column = ''
    if len(sql_list) > 1:
        sql = " left join ".join(sql_list) + " on t0.search_word = t1.search_word "
        hb_column = column_tbhbtype_value[0]
        hb_column_alias = tbhb_column[0]

        hb_column = ", case when t1.{hb_column} <>0 then round((t0.{hb_column} - t1.{hb_column}) / t1.{hb_column} *100 ,2) " \
                    "else null end as ".format(hb_column=hb_column) + hb_column_alias
    else:
        sql = sql_list[0]
    final_column = "t0.*" + hb_column
    sql = "select " + final_column + " from " + sql + " where " + yidong_where

    #sum
    sum_columns =','.join(["sum("+ele+")" for ele in findy_column_upper if not ele.endswith('Rate')])

    sum_sql = " select "+sum_columns +" from ("+sql+") t"
    ck_db = PyCK()
    sql_result = ck_db.get_result_from_db(sum_sql)


    if set(sql_result[0]) == {0}:
        sql_result = []

    df = pd.DataFrame(sql_result, columns=df_column)
    return df



def do_ciyun(date,starttime,endtime,s):
    '''
    词云：
    :return:
    '''
    url = "http://test.newwk.dangdang.com/api/v3/reportForm/realtime/searchwordCiYun"

    # 遍历前端筛选条件
    filter_list = get_all_filters(date, starttime, endtime)

    len1 = len(filter_list)
    print(len1)
    api_request = ApiHandle(token)

    for filter in filter_list:
        data = copy.deepcopy(filter)

        date_str = data['queryDate']
        date_list = [date_str]
        data['sort'] = 'desc'
        data['pageSize'] = 50
        for item in ['searchPv','searchUv', 'clickPv', 'clickUv']:
            data['orderBy'] = item
            print(data)
            #debug

            # data = {'searchword': '', 'anomaly': [{'relation': '', 'field': 'suv', 'type': 'hb', 'symbol': '>=', 'value': '88%'},
            #                                       {'relation': 'and', 'field': 'samt_rate', 'type': 'number', 'symbol': '>=', 'value': '10'}],
            #         'anomalyValue': [{'relation': '', 'field': 'suv', 'type': 'hb', 'symbol': '>=', 'value': '88%'},
            #                          {'relation': 'and', 'field': 'samt_rate', 'type': 'number', 'symbol': '>=', 'value': '10'}],
            #         'compareType': '[1, 2, 3]', 'queryDate': '2021-11-24', 'startTime': '00:00', 'endTime': '11:00', 'pageNo': 1,
            #         'pageSize': 50, 'sort': 'desc', 'orderBy': 'clickPv', 'bdId': '0'}
            #
            # item = data['orderBy']

            req = api_request.post(url=url, data=data)
            api_data_dict = json.loads(req.decode('utf-8'))
            api_data_list = api_data_dict['payload']['modelList']  # 无返回数据 apiresult_list =[]
            total_num = int(api_data_dict['payload']['total'])


            api_data = {ele['searchWord']:eval(ele[item]) for ele in api_data_list}

            #sql 结果
            sql_data = get_sql_ciyun_data(data)

            #diff
            diff_value = diff(sql_data, api_data)
            if diff_value !={}:
                real_search_logger.info('-Fail-筛选条件'+str(data))
                real_search_logger.info(diff_value)
            else:
                real_search_logger.info('-Pass-筛选条件' + str(data))
            real_search_logger.info(' ')


def do_wordline_job(date,starttime,endtime,token):
    '''
    折线图
    :param date:
    :param starttime:
    :param endtime:
    :param s:
    :return:
    '''
    url ="http://test.newwk.dangdang.com/api/v3/reportForm/realtime/searchwordLine"

    findby_list = ['search_pv', 'search_uv', 'click_pv', 'click_uv', 'search_click_rate', 'search_cust_rate']

    date_list = [datetime.strftime(datetime.strptime(date,'%Y-%m-%d') - datetime.timedelta(days=i),'%Y-%m-%d')  for i in range(8)][0:]
    # 遍历前端筛选条件
    filter_list = get_all_filters(date, starttime, endtime)

    len1 = len(filter_list)
    print(len1)

    #
    all_com = [list(combinations(findby_list, i)) for i in range(1, 5)]
    findby_iter = list(chain.from_iterable(all_com))

    api_request = ApiHandle(token)

    for filter in filter_list:
        data = dict(filter)

        #debug
        data ={'searchword': '', 'anomaly': [{'relation': '', 'field': 'suv', 'type': 'hb_lastweek', 'symbol': '>=', 'value': '69%'}],
              'anomalyValue': [{'relation': '', 'field': 'suv', 'type': 'hb_lastweek', 'symbol': '>=', 'value': '69%'}],
              'compareType': '[1, 2, 3]', 'queryDate': '2021-11-26', 'startTime': '00:00', 'endTime': '24:00',
              'pageNo': 1, 'pageSize': 10, 'sort': '', 'orderBy': '', 'bdId': '2'}

        data['endTime'] = "24:00"

        for item in findby_iter[0:1]:
            data['findBy'] = str(list(item))
            print(data)
            #api

            req = api_request.post(url = url, data = data)
            api_data_dict = json.loads(req.decode('utf-8'))
            apiresult_list = api_data_dict['payload']['modelList']  # 无返回数据 apiresult_list =[]
            total_num = int(api_data_dict['payload']['total'])

            for key,value in api_data_dict.items():
                if isinstance(value,list):
                    api_data_dict[key] = {key:value for ele in value for key,value in ele.items()}

            #clickhouse
            df = pd.DataFrame([],columns = [])
            data_list_zip = []
            for each_date in date_list:
                data['queryDate'] = each_date
                temp_df = get_sql_wordline_data(data)

                if not temp_df.empty:
                    data_list_zip.append(each_date)
                    df = df.append(temp_df, ignore_index=True)

            sql_data_dict = {}
            if not df.empty:
                df = df.apply(pd.to_numeric, errors='ignore')
                df['search_click_rate'] = round(df['click_uv'] / df['search_uv'], 2)
                df['search_cust_rate'] = round(df['createCustNum'] / df['search_uv'], 2)

                for ele in findby_list:
                    value = df[ele].values.tolist()
                    sql_data_dict[ele] = dict(zip(data_list_zip,value))
            else:
                sql_data_dict ={ele:{} for ele in findby_list}

            #diff
            message = '-Success-'
            diffvalue = ''

            diff_value = diff(sql_data_dict, api_data_dict)

            filter_output = dict(data)
            if diff_value != {}:
                message = '-Fail-'
                diffvalue = diff_value

                real_search_logger.info(message + " 筛选条件:" + str(filter_output))
                real_search_logger.info(str(diffvalue))
                real_search_logger.info('\n')

    pass



if __name__ == '__main__':


    a = ''
    if a == '':
        a = datetime.now()
        date = datetime.strftime(a, "%Y-%m-%d")
        now_hour = a.hour
    else:
        date = a
        now_hour = 12

    starttime = '00:00'

    if now_hour < 10:
        endtime = "0" + str(now_hour) + ":00"
    else:
        endtime = str(now_hour) + ":00"

    token=""

    #
    do_job(date,starttime,endtime,token)
    do_ciyun(date,starttime,endtime,token)
    do_wordline_job(date, starttime, endtime, token)
