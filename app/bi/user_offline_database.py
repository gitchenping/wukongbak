import sys
import platform

from .util import *
from .public_function import *
import re


sd_cust_sql="""groupBitmapMerge(cust_id_state) as create_parent_uv_sd,
round(groupBitmapMerge(parent_id_state)/groupBitmapMerge(cust_id_state), 2) as daycount_ratio_sd
FROM bi_mdata.dm_order_create_day
WHERE {date_str}
AND `source` IN {source}
AND platform IN {platform}"""

sd_new_cust_sql="""groupBitmapMerge(cust_id_state) as new_create_parent_uv_sd
FROM bi_mdata.dm_order_create_day
WHERE {date_str}
AND `source` IN {source}
AND platform IN {platform}
AND day_new_flag =1"""

sd_cust_trip_sql="""select {select_case_when} as case_name,
{sd_basic_sql}
AND {filter_info}
group by case_name"""

zf_cust_sql="""groupBitmapMerge(cust_id_state) as create_parent_uv_zf,
round(groupBitmapMerge(parent_id_state)/groupBitmapMerge(cust_id_state), 2) as daycount_ratio_zf
FROM bi_mdata.dm_order_pay_day
WHERE {date_str}
AND `source` IN {source}
AND platform IN {platform}"""

zf_new_cust_sql="""groupBitmapMerge(cust_id_state) as create_parent_uv_zf
FROM bi_mdata.dm_order_pay_day
WHERE {date_str}
AND `source` IN {source}
AND platform IN {platform}
AND day_new_flag =1"""

zf_cust_trip_sql="""select {select_case_when} as case_name,
{zf_basic_sql}
AND {filter_info}
group by case_name"""

ck_cust_sql="""groupBitmapMerge(cust_id_state) as create_parent_uv_ck
FROM bi_mdata.dm_order_send_day
WHERE {date_str}
AND `source` IN {source}
AND platform IN {platform}"""

ck_new_cust_sql="""groupBitmapMerge(cust_id_state) as new_create_parent_uv_ck
FROM bi_mdata.dm_order_send_day
WHERE {date_str}
AND `source` IN {source}
AND platform IN {platform}
AND day_new_flag =1"""

ck_cust_trip_sql="""select {select_case_when} as case_name,
{ck_basic_sql}
AND {filter_info}
group by case_name"""


def get_index_result(start, end, source, platform):
    conn=ck_connect()
    date_str=get_day_str(start, end)
    print("SELECT" + ' ' + sd_new_cust_sql.format(date_str=date_str, source=source, platform=platform))
    sd_new_result=conn.execute("SELECT" + ' ' + sd_new_cust_sql.format(date_str=date_str, source=source, platform=platform))
    zf_new_result=conn.execute("SELECT" + ' ' + zf_new_cust_sql.format(date_str=date_str, source=source, platform=platform))
    ck_new_result=conn.execute("SELECT" + ' ' + ck_new_cust_sql.format(date_str=date_str, source=source, platform=platform))
    sd_result=conn.execute("SELECT" + ' ' + sd_cust_sql.format(date_str=date_str, source=source, platform=platform))
    zf_result=conn.execute("SELECT" + ' ' + zf_cust_sql.format(date_str=date_str, source=source, platform=platform))
    ck_result=conn.execute("SELECT" + ' ' + ck_cust_sql.format(date_str=date_str, source=source, platform=platform))
    results_map={
                 'new_create_parent_uv_sd': format_person_num(sd_new_result[0][0]),
                 'new_create_parent_uv_zf': format_person_num(zf_new_result[0][0]),
                 'new_create_parent_uv_ck': format_person_num(ck_new_result[0][0]),
                 'create_parent_uv_sd': format_person_num(sd_result[0][0]),
                 'create_parent_uv_zf': format_person_num(zf_result[0][0]),
                 'create_parent_uv_ck': format_person_num(ck_result[0][0]),
                 'daycount_ratio_sd': sd_result[0][1],
                 'daycount_ratio_zf': zf_result[0][1]
                 }
    return results_map


def get_index_ratio_result(start, end, y_start, y_end, source, platform):
    """
     :param start: 当前计算开始时间
    :param end: 当前计算结束时间
    :param y_start: 同环比的开始时间
    :param y_end: 同环比的结束时间
    :param date_type: 日期类型：日周月季
    :return:
    """
    conn = ck_connect()
    date_str_today=get_day_str(start, end)
    date_str_yesterday=get_day_str(y_start, y_end)
    sd_new_result=conn.execute("SELECT"+ ' ' + sd_new_cust_sql.format(date_str=date_str_today, source=source, platform=platform))
    zf_new_result=conn.execute("SELECT" + ' '+ zf_new_cust_sql.format(date_str=date_str_today, source=source, platform=platform))
    ck_new_result=conn.execute("SELECT"+ ' ' + ck_new_cust_sql.format(date_str=date_str_today, source=source, platform=platform))
    sd_result=conn.execute("SELECT" + ' '+ sd_cust_sql.format(date_str=date_str_today, source=source, platform=platform))
    zf_result=conn.execute("SELECT" + ' '+ zf_cust_sql.format(date_str=date_str_today, source=source, platform=platform))
    ck_result=conn.execute("SELECT" + ' '+ ck_cust_sql.format(date_str=date_str_today, source=source, platform=platform))

    sd_old_result=conn.execute("SELECT" + ' '+ sd_new_cust_sql.format(date_str=date_str_yesterday, source=source, platform=platform))
    zf_old_result=conn.execute("SELECT" + ' '+ zf_new_cust_sql.format(date_str=date_str_yesterday, source=source, platform=platform))
    ck_old_result=conn.execute("SELECT" + ' '+ ck_new_cust_sql.format(date_str=date_str_yesterday, source=source, platform=platform))
    sd_result_old=conn.execute("SELECT" + ' '+ sd_cust_sql.format(date_str=date_str_yesterday, source=source, platform=platform))
    zf_result_old=conn.execute("SELECT" + ' '+ zf_cust_sql.format(date_str=date_str_yesterday, source=source, platform=platform))
    ck_result_old=conn.execute("SELECT" + ' '+ ck_cust_sql.format(date_str=date_str_yesterday, source=source, platform=platform))

    if len(sd_new_result) > 0 and len(sd_old_result) > 0 and sd_old_result[0][0] != 0:
        new_create_parent_uv_sd = format_ratio((sd_new_result[0][0] - sd_old_result[0][0]) / sd_old_result[0][0]*100)
    else:
        new_create_parent_uv_sd ='--'
    if len(zf_new_result) > 0 and len(zf_old_result) > 0 and zf_old_result[0][0] != 0:
        new_create_parent_uv_zf = format_ratio((zf_new_result[0][0] - zf_old_result[0][0]) / zf_old_result[0][0]*100)
    else:
        new_create_parent_uv_zf ='--'
    if len(ck_new_result) > 0 and len(ck_old_result) > 0 and ck_old_result[0][0] != 0:
        new_create_parent_uv_ck =format_ratio((ck_new_result[0][0] - ck_old_result[0][0]) / ck_old_result[0][0]*100)
    else:
        new_create_parent_uv_ck ='--'
    if len(sd_result) > 0 and len(sd_result_old) > 0 and sd_result_old[0][0] != 0:
        create_parent_uv_sd = format_ratio((sd_result[0][0] - sd_result_old[0][0]) / sd_result_old[0][0]*100)
    else:
        create_parent_uv_sd ='--'
    if len(zf_result) > 0 and len(zf_result_old) > 0 and zf_result_old[0][0] != 0:
        create_parent_uv_zf = format_ratio((zf_result[0][0] - zf_result_old[0][0]) / zf_result_old[0][0]*100)
    else:
        create_parent_uv_zf ='--'
    if len(ck_result) > 0 and len(ck_result_old) > 0 and ck_result_old[0][0] != 0:
        create_parent_uv_ck = format_ratio((ck_result[0][0] - ck_result_old[0][0]) / ck_result_old[0][0]*100)
    else:
        create_parent_uv_ck ='--'
    if len(sd_result) > 0 and len(sd_result_old) > 0 and sd_result_old[0][1] != 0:
        daycount_ratio_sd = format_ratio((sd_result[0][1] - sd_result_old[0][1]) / sd_result_old[0][1]*100)
    else:
        daycount_ratio_sd ='--'
    if len(zf_result) > 0 and len(zf_result_old) > 0 and zf_result_old[0][1] != 0:
        daycount_ratio_zf = format_ratio((zf_result[0][1] - zf_result_old[0][1]) / zf_result_old[0][1]*100)
    else:
        daycount_ratio_zf ='--'
    results_map = {
                   'new_create_parent_uv_sd': new_create_parent_uv_sd,
                   'new_create_parent_uv_zf': new_create_parent_uv_zf,
                   'new_create_parent_uv_ck': new_create_parent_uv_ck,
                   'create_parent_uv_sd': create_parent_uv_sd,
                   'create_parent_uv_zf': create_parent_uv_zf,
                    'create_parent_uv_ck': create_parent_uv_ck,
                   'daycount_ratio_sd': daycount_ratio_sd,
                   'daycount_ratio_zf': daycount_ratio_zf
                  }
    return results_map


def get_bd_result(start, end, platform, source, bd_id,  field_str, date_type):
    date_str=get_date_str(start, end, date_type, field_str)
    if bd_id == '(5,12,1,3,4,9,15,16,6,20,21,22,23,0,13,2)':
        if 'new_create_parent_uv_sd' == field_str:
            sql=sd_cust_trip_sql.format(select_case_when=bd_case_when,  sd_basic_sql=
                                        sd_new_cust_sql.format(date_str=date_str, source=source, platform=platform),
                                         filter_info ='bd_id in {bd_id}'.format(bd_id=bd_id))
        elif field_str == 'create_parent_uv_sd' or field_str == 'daycount_ratio_sd':
            sql=sd_cust_trip_sql.format(select_case_when=bd_case_when, sd_basic_sql=
                                        sd_cust_sql.format(date_str=date_str, source=source, platform=platform),
                                        filter_info='bd_id in {bd_id}'.format(bd_id=bd_id))
        elif 'new_create_parent_uv_zf' == field_str:
            sql=zf_cust_trip_sql.format(select_case_when=bd_case_when,  zf_basic_sql=
                                        zf_new_cust_sql.format(date_str=date_str, source=source, platform=platform),
                                         filter_info ='bd_id in {bd_id}'.format(bd_id=bd_id))
        elif 'create_parent_uv_zf' == field_str or 'daycount_ratio_zf' == field_str:
            sql=zf_cust_trip_sql.format(select_case_when=bd_case_when,  zf_basic_sql=
                                        zf_cust_sql.format(date_str=date_str, source=source, platform=platform),
                                         filter_info ='bd_id in {bd_id}'.format(bd_id=bd_id))
        elif 'new_create_parent_uv_ck' == field_str:
            sql=ck_cust_trip_sql.format(select_case_when=bd_case_when,  ck_basic_sql=
                                        ck_new_cust_sql.format(date_str=date_str, source=source, platform=platform),
                                         filter_info ='bd_id in {bd_id}'.format(bd_id=bd_id))
        elif 'create_parent_uv_ck' == field_str:
            sql=ck_cust_trip_sql.format(select_case_when=bd_case_when,  ck_basic_sql=
                                        ck_cust_sql.format(date_str=date_str, source=source, platform=platform),
                                         filter_info ='bd_id in {bd_id}'.format(bd_id=bd_id))
        print(sql)
        conn=ck_connect()
        result=conn.execute(sql)
        if len(result) > 0:
            if field_str == 'daycount_ratio_sd' or field_str == 'daycount_ratio_zf':
                result_map={i[0]: i[2] for i in result}
                return result_map
            else:
                result_map={i[0]: format_person_num(i[1]) for i in result}
                return result_map
        else:
            return {}
    elif bd_id == '(5,12)':
        if 'new_create_parent_uv_sd' == field_str:
            sql=sd_cust_trip_sql.format(select_case_when=categorypath3_case_when,  sd_basic_sql=
                                        sd_new_cust_sql.format(date_str=date_str, source=source, platform=platform),
                                        filter_info='bd_id in {bd_id}'.format(bd_id=bd_id))
        elif field_str == 'create_parent_uv_sd' or field_str == 'daycount_ratio_sd':
            sql=sd_cust_trip_sql.format(select_case_when=categorypath3_case_when, sd_basic_sql=
                                        sd_cust_sql.format(date_str=date_str, source=source, platform=platform),
                                        filter_info='bd_id in {bd_id}'.format(bd_id=bd_id))
        elif 'new_create_parent_uv_zf' == field_str:
            sql=zf_cust_trip_sql.format(select_case_when=categorypath3_case_when,  zf_basic_sql=
                                        zf_new_cust_sql.format(date_str=date_str, source=source, platform=platform),
                                         filter_info ='bd_id in {bd_id}'.format(bd_id=bd_id))
        elif field_str == 'create_parent_uv_zf' or field_str == 'daycount_ratio_zf':
            sql=zf_cust_trip_sql.format(select_case_when=categorypath3_case_when,  zf_basic_sql=
                                        zf_cust_sql.format(date_str=date_str, source=source, platform=platform),
                                         filter_info ='bd_id in {bd_id}'.format(bd_id=bd_id))
        elif 'new_create_parent_uv_ck' == field_str:
            sql=ck_cust_trip_sql.format(select_case_when=categorypath3_case_when,  ck_basic_sql=
                                        ck_new_cust_sql.format(date_str=date_str, source=source, platform=platform),
                                         filter_info ='bd_id in {bd_id}'.format(bd_id=bd_id))
        elif 'create_parent_uv_ck' == field_str:
            sql=ck_cust_trip_sql.format(select_case_when=categorypath3_case_when,  ck_basic_sql=
                                        ck_cust_sql.format(date_str=date_str, source=source, platform=platform),
                                         filter_info ='bd_id in {bd_id}'.format(bd_id=bd_id))
        print(sql)
        conn=ck_connect()
        result=conn.execute(sql)
        if len(result) > 0:
            if field_str == 'daycount_ratio_sd' or field_str == 'daycount_ratio_zf':
                result_map={i[0]: i[2] for i in result}
                return result_map
            else:
                result_map={i[0]: format_person_num(i[1]) for i in result}
                return result_map
        else:
            return {}


def get_customer_result(start, end, platform, source, field_str, date_type):
    """日周月季的新老客标识字段不是唯一的，所以进行判断"""
    date_str=get_date_str(start, end, date_type, field_str)
    if date_type == 'd':
        if field_str == 'create_parent_uv_sd' or field_str == 'daycount_ratio_sd':
            sql=sd_cust_trip_sql.format(select_case_when=customer_case_when.format(new_flag='day_new_flag'),
                                        sd_basic_sql=sd_cust_sql.format(date_str=date_str,source=source, platform=platform),
                                        filter_info='1=1')
        elif field_str == 'create_parent_uv_zf' or field_str == 'daycount_ratio_zf':
            sql=zf_cust_trip_sql.format(select_case_when=customer_case_when.format(new_flag='day_new_flag'),
                                        zf_basic_sql=zf_cust_sql.format(date_str=date_str, source=source,
                                                                        platform=platform),
                                        filter_info='1=1')
        elif field_str == 'create_parent_uv_ck':
            sql=ck_cust_trip_sql.format(select_case_when=customer_case_when.format(new_flag='day_new_flag'),
                                        ck_basic_sql=ck_cust_sql.format(date_str=date_str, source=source,
                                                                        platform=platform),
                                        filter_info='1=1')
    elif date_type == 'w':
        if field_str == 'create_parent_uv_sd' or field_str == 'daycount_ratio_sd':
            sql=sd_cust_trip_sql.format(select_case_when=customer_case_when.format(new_flag='week_new_flag'),
                                        sd_basic_sql=sd_cust_sql.format(date_str=date_str,source=source, platform=platform),
                                        filter_info='1=1')
        elif field_str == 'create_parent_uv_zf' or field_str == 'daycount_ratio_zf':
            sql=zf_cust_trip_sql.format(select_case_when=customer_case_when.format(new_flag='week_new_flag'),
                                        zf_basic_sql=zf_cust_sql.format(date_str=date_str, source=source,
                                                                        platform=platform),
                                        filter_info='1=1')
        elif field_str == 'create_parent_uv_ck':
            sql=ck_cust_trip_sql.format(select_case_when=customer_case_when.format(new_flag='week_new_flag'),
                                        ck_basic_sql=ck_cust_sql.format(date_str=date_str, source=source,
                                                                        platform=platform),
                                        filter_info='1=1')

    elif date_type == 'm':
        if field_str == 'create_parent_uv_sd' or field_str == 'daycount_ratio_sd':
            sql=sd_cust_trip_sql.format(select_case_when=customer_case_when.format(new_flag='month_new_flag'),
                                        sd_basic_sql=sd_cust_sql.format(date_str=date_str, source=source,
                                                                        platform=platform),
                                        filter_info='1=1')
        elif field_str == 'create_parent_uv_zf' or field_str == 'daycount_ratio_zf':
            sql=zf_cust_trip_sql.format(select_case_when=customer_case_when.format(new_flag='month_new_flag'),
                                        zf_basic_sql=zf_cust_sql.format(date_str=date_str, source=source,
                                                                        platform=platform),
                                        filter_info='1=1')
        elif field_str == 'create_parent_uv_ck':
            sql=ck_cust_trip_sql.format(select_case_when=customer_case_when.format(new_flag='month_new_flag'),
                                        ck_basic_sql=ck_cust_sql.format(date_str=date_str, source=source,
                                                                        platform=platform),
                                        filter_info='1=1')

    elif date_type == 'q':
        if field_str == 'create_parent_uv_sd' or field_str == 'daycount_ratio_sd':
            sql=sd_cust_trip_sql.format(select_case_when=customer_case_when.format(new_flag='quarter_new_flag'),
                                        sd_basic_sql=sd_cust_sql.format(date_str=date_str, source=source,
                                                                        platform=platform),
                                        filter_info='1=1')
        elif field_str == 'create_parent_uv_zf' or field_str == 'daycount_ratio_zf':
            sql=zf_cust_trip_sql.format(select_case_when=customer_case_when.format(new_flag='quarter_new_flag'),
                                        zf_basic_sql=zf_cust_sql.format(date_str=date_str, source=source,
                                                                        platform=platform),
                                        filter_info='1=1')
        elif field_str == 'create_parent_uv_ck':
            sql=ck_cust_trip_sql.format(select_case_when=customer_case_when.format(new_flag='quarter_new_flag'),
                                        ck_basic_sql=ck_cust_sql.format(date_str=date_str, source=source,
                                                                        platform=platform),
                                        filter_info='1=1')
    print(sql)
    conn=ck_connect()
    result=conn.execute(sql)
    if len(result) != 0:
        if field_str == 'daycount_ratio_sd' or field_str == 'daycount_ratio_zf':
            result_map={i[0]: i[2] for i in result}
            return result_map
        else:
            result_map={i[0]: format_person_num(i[1])for i in result}
            return result_map
    else:
        return {}


def get_trend_result(start, end, platform, source, bd_id,field_str, date_type):
    if date_type == 'd':
        date_str=get_day_str(start, end)
        select_case_when='date_str'
    elif date_type == 'w':
        date_str=get_date_str(start, end, date_type, 'trend')
        select_case_when='toStartOfWeek(date_str,1)'
    elif date_type == 'm':
        date_str=get_date_str(start, end, date_type, 'trend')
        select_case_when='toStartOfMonth(date_str)'
    elif date_type == 'q':
        date_str=get_date_str(start, end, date_type, 'trend')
        select_case_when='toStartOfQuarter(date_str)'

    if field_str == 'create_parent_uv_sd' or field_str == 'daycount_ratio_sd':
        sql=sd_cust_trip_sql.format(select_case_when=select_case_when,
                                    sd_basic_sql=sd_cust_sql.format(date_str=date_str, source=source,
                                                                    platform=platform),
                                    filter_info='bd_id in {}'.format(bd_id))
    elif field_str == 'new_create_parent_uv_sd':
        sql=sd_cust_trip_sql.format(select_case_when=select_case_when,
                                    sd_basic_sql=sd_new_cust_sql.format(date_str=date_str, source=source,
                                                                        platform=platform),
                                    filter_info='bd_id in {}'.format(bd_id))
    elif field_str == 'create_parent_uv_zf' or field_str == 'daycount_ratio_zf':
        sql=zf_cust_trip_sql.format(select_case_when=select_case_when,
                                    zf_basic_sql=zf_cust_sql.format(date_str=date_str, source=source,
                                                                    platform=platform),
                                    filter_info='bd_id in {}'.format(bd_id))
    elif field_str == 'new_create_parent_uv_zf':
        sql=zf_cust_trip_sql.format(select_case_when=select_case_when,
                                    zf_basic_sql=zf_new_cust_sql.format(date_str=date_str, source=source,
                                                                        platform=platform),
                                    filter_info='bd_id in {}'.format(bd_id))
    elif field_str == 'create_parent_uv_ck':
        sql=ck_cust_trip_sql.format(select_case_when=select_case_when,
                                    ck_basic_sql=ck_cust_sql.format(date_str=date_str, source=source,
                                                                    platform=platform),
                                    filter_info='bd_id in {}'.format(bd_id))
    elif field_str == 'new_create_parent_uv_ck':
        sql=ck_cust_trip_sql.format(select_case_when=select_case_when,
                                    ck_basic_sql=ck_new_cust_sql.format(date_str=date_str, source=source,
                                                                        platform=platform),
                                    filter_info='bd_id in {}'.format(bd_id))

    print(sql)
    conn=ck_connect()
    result=conn.execute(sql)
    if len(result) != 0:
        if field_str == 'daycount_ratio_sd' or field_str == 'daycount_ratio_zf':
            if date_type == 'd':
                result_map={format_date_by_day(i[0]): i[2] for i in result}
            elif date_type == 'w':
                result_map={get_week_by_date(i[0]): i[2] for i in result}
            elif date_type == 'm':
                result_map={get_month_by_date(i[0]): i[2] for i in result}
            elif date_type == 'q':
                result_map={get_quarter_by_date(i[0]): i[2] for i in result}
            return result_map
        else:
            if date_type == 'd':
                result_map={format_date_by_day(i[0]): str(i[1]) for i in result}
            elif date_type == 'w':
                result_map={get_week_by_date(i[0]): str(i[1]) for i in result}
            elif date_type == 'm':
                result_map={get_month_by_date(i[0]): str(i[1]) for i in result}
            elif date_type == 'q':
                result_map={get_quarter_by_date(i[0]): str(i[1]) for i in result}
            return result_map
    else:
        return {}


def get_platform_result(start, end, platform, source,  field_str, date_type):
    date_str=get_date_str(start, end, date_type, field_str)
    if source == '(1,2,3,4)':
        if field_str == 'new_create_parent_uv_sd':
            sql=sd_cust_trip_sql.format(select_case_when=source_case_when,
                                   sd_basic_sql=sd_new_cust_sql.format(date_str=date_str, source=source,
                                                                        platform=platform), filter_info='1=1')
        elif field_str == 'create_parent_uv_sd' or field_str == 'daycount_ratio_sd':
            sql=sd_cust_trip_sql.format(select_case_when=source_case_when,
                                   sd_basic_sql=sd_cust_sql.format(date_str=date_str, source=source,
                                    platform=platform),filter_info='1=1')

        elif field_str == 'create_parent_uv_zf' or field_str == 'daycount_ratio_zf':
            sql=zf_cust_trip_sql.format(select_case_when=source_case_when,
                                        zf_basic_sql=zf_cust_sql.format(date_str=date_str, source=source,
                                        platform=platform), filter_info='1=1')
        elif field_str == 'new_create_parent_uv_zf':
            sql=zf_cust_trip_sql.format(select_case_when=source_case_when,
                                        zf_basic_sql=zf_new_cust_sql.format(date_str=date_str, source=source,
                                        platform=platform), filter_info='1=1')
        elif field_str == 'new_create_parent_uv_ck':
            sql=ck_cust_trip_sql.format(select_case_when=source_case_when,
                                        ck_basic_sql=ck_new_cust_sql.format(date_str=date_str, source=source,
                                        platform=platform), filter_info='1=1')
        elif field_str == 'create_parent_uv_ck':
            sql=ck_cust_trip_sql.format(select_case_when=source_case_when,
                                        ck_basic_sql=ck_cust_sql.format(date_str=date_str, source=source,
                                        platform=platform), filter_info='1=1')
        print(sql)
        conn=ck_connect()
        result=conn.execute(sql)
        if len(result) != 0:
            if field_str != 'daycount_ratio_sd' and field_str != 'daycount_ratio_zf':
                result_map = {i[0]: format_person_num(i[1]) for i in result}
                return result_map
            else:
                result_map = {i[0]: i[2] for i in result}
                return result_map
        else:
            return {}
    else:
        if platform == '(1,2,3,4,5,6,7,8,9,12,20,0)':
            if field_str == 'create_parent_uv_sd' or field_str == 'daycount_ratio_sd':
                sql=sd_cust_trip_sql.format(select_case_when=platform_case_when,
                                       sd_basic_sql=sd_cust_sql.format(date_str=date_str, source=source,
                                                                            platform=platform), filter_info='1=1')
            elif field_str == 'new_create_parent_uv_sd':
                sql=sd_cust_trip_sql.format(select_case_when=platform_case_when,
                                       sd_basic_sql=sd_new_cust_sql.format(date_str=date_str, source=source,
                                                                            platform=platform), filter_info='1=1')
            elif field_str == 'create_parent_uv_zf' or field_str == 'daycount_ratio_zf':
                sql=zf_cust_trip_sql.format(select_case_when=platform_case_when,
                                            zf_basic_sql=zf_cust_sql.format(date_str=date_str, source=source,
                                                                            platform=platform), filter_info='1=1')
            elif field_str == 'new_create_parent_uv_zf' :
                sql=zf_cust_trip_sql.format(select_case_when=platform_case_when,
                                            zf_basic_sql=zf_new_cust_sql.format(date_str=date_str, source=source,
                                                                            platform=platform), filter_info='1=1')
            elif field_str == 'create_parent_uv_ck':
                sql=ck_cust_trip_sql.format(select_case_when=platform_case_when,
                                            ck_basic_sql=ck_cust_sql.format(date_str=date_str, source=source,
                                                                            platform=platform), filter_info='1=1')
            elif field_str == 'new_create_parent_uv_ck':
                sql=ck_cust_trip_sql.format(select_case_when=platform_case_when,
                                            ck_basic_sql=ck_new_cust_sql.format(date_str=date_str, source=source,
                                                                            platform=platform), filter_info='1=1')
        elif platform == '(1,2)':
            if field_str == 'create_parent_uv_sd' or field_str == 'daycount_ratio_sd':
                sql=sd_cust_trip_sql.format(select_case_when=app_case_when,
                                       sd_basic_sql=sd_cust_sql.format(date_str=date_str, source=source,
                                                                            platform=platform), filter_info='1=1')
            elif field_str == 'new_create_parent_uv_sd':
                sql=sd_cust_trip_sql.format(select_case_when=app_case_when,
                                       sd_basic_sql=sd_new_cust_sql.format(date_str=date_str, source=source,
                                                                            platform=platform), filter_info='1=1')
            elif field_str == 'create_parent_uv_zf' or field_str == 'daycount_ratio_zf':
                sql=zf_cust_trip_sql.format(select_case_when=app_case_when,
                                            zf_basic_sql=zf_cust_sql.format(date_str=date_str, source=source,
                                                                            platform=platform), filter_info='1=1')
            elif field_str == 'new_create_parent_uv_zf' :
                sql=zf_cust_trip_sql.format(select_case_when=app_case_when,
                                            zf_basic_sql=zf_new_cust_sql.format(date_str=date_str, source=source,
                                                                            platform=platform), filter_info='1=1')
            elif field_str == 'create_parent_uv_ck':
                sql=ck_cust_trip_sql.format(select_case_when=app_case_when,
                                            ck_basic_sql=ck_cust_sql.format(date_str=date_str, source=source,
                                                                            platform=platform), filter_info='1=1')
            elif field_str == 'new_create_parent_uv_ck':
                sql=ck_cust_trip_sql.format(select_case_when=app_case_when,
                                            ck_basic_sql=ck_new_cust_sql.format(date_str=date_str, source=source,
                                                                            platform=platform), filter_info='1=1')
        print(sql)
        conn=ck_connect()
        result=conn.execute(sql)
        if len(result) != 0:
            if field_str != 'daycount_ratio_sd' and field_str != 'daycount_ratio_zf':
                result_map={i[0]: format_person_num(i[1]) for i in result}
                return result_map
            else:
                result_map={i[0]: i[2] for i in result}
                return result_map
        else:
            return {}


def get_zb_result(start, end, platform, source, field_str, result, date_type):
    """ 计算下钻页的占比数据"""
    date_str=get_date_str(start, end, date_type, field_str)
    if field_str == 'create_parent_uv_sd' or field_str == 'daycount_ratio_sd':
        sql = "SELECT" + ' ' + sd_cust_sql.format(date_str=date_str, source=source,platform=platform)
    elif field_str == 'new_create_parent_uv_sd':
        sql = "SELECT" + ' ' + sd_new_cust_sql.format(date_str=date_str, source=source,platform=platform)
    elif field_str == 'create_parent_uv_zf' or field_str == 'daycount_ratio_zf':
        sql = "SELECT" + ' ' + zf_cust_sql.format(date_str=date_str, source=source, platform=platform)
    elif field_str == 'new_create_parent_uv_zf':
        sql = "SELECT" + ' ' + zf_new_cust_sql.format(date_str=date_str, source=source,platform=platform)
    elif field_str == 'create_parent_uv_ck':
        sql = "SELECT" + ' ' + ck_cust_sql.format(date_str=date_str, source=source,platform=platform)
    elif field_str == 'new_create_parent_uv_ck':
        sql = "SELECT" + ' ' + ck_new_cust_sql.format(date_str=date_str, source=source,platform=platform)
    conn=ck_connect()
    total_amt=conn.execute(sql)
    print(total_amt)
    print(result)
    if total_amt[0][0] != 0:
        if field_str == 'daycount_ratio_sd' or field_str == 'daycount_ratio_zf':
            value_zb = {}
        else:
            value_zb = {i[0]: format_ratio(float(i[1]) / total_amt[0][0] * 100) for i in result.items()}
    else:
        value_zb = {i[0]:'--' for i in result.items()}
    return value_zb

if __name__ == '__main__':
    # do_compare_job()
    # result = get_bd_result('2020-12-16','15',"(1)","(1)","(1)","(1)")
    # print(result)
    date_str='2020-10-25'
    last_quarter_list = get_quarterdays_by_num(date_str,2)
    y_date = get_end_time(date_str, -1)
    start=last_quarter_list[0][0]
    end=last_quarter_list[0][1]
    # platform = '(1,2,3,4,5,6,7,8,9,12,20,0)'
    platform = '(1,2)'
    source = '(1)'
    bd_id= '(5,12)'
    # bd_id = '(5,12,1,3,4,9,15,16,6,20,21,22,23,0,13,2)'
    shop_type = '(1)'
    date_type='w'
    field_str='create_parent_uv_sd'
    #field_str='create_parent_uv_ck'

    # source ="('1', '2', '3', '4')"
    # platform = "('1', '2', '3', '4', '5', '6', '7', '8', '9', '12', '20', '0')"

    url = """http://192.168.105.85:8085/api/overview_v5?action=user_analysis&view=trend&field_str=create_parent_uv_sd
    &date_type=d&date=2020-12-27&shop_type=&eliminate_type=&source=1&parent_platform=1&platform=all&sale_type=&bd_id=1"""
    # result = get_bd_result(start, end, platform, source, bd_id, shop_type, field_str,date_type)
    #
    # print(result)
    # r = get_bd_result(start, end, platform, source, bd_id,  field_str, date_type)
    r = get_customer_result(start, end, platform, source, field_str, date_type)
    # r = get_trend_result(start, end, platform, source, field_str, date_type)
    # r = get_platform_result(start, end, platform, source,  field_str, date_type)
    # print(r)
    for i in r.items():
        print(i)
    t = get_zb_result(start, end, platform, source, field_str, r, date_type)
    print(t)
