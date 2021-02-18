#!/usr/bin/env python
# !encoding=utf8
import os
import sys
import platform
from utils import util

"""
该脚本用于验证移动悟空离线数据-用户分析-各个用户数据模块
"""
sys.path.append("..")
cwd = os.getcwd()
parent = os.path.dirname(cwd)
father_path = os.path.abspath(parent + os.path.sep + "..")
sys.path.append(father_path)
import requests
import json
import logging.config
from .bi.util import *
from .bi.user_offline_database import *
from .bi.public_function import *

logging.config.fileConfig("./app/bi/conf/logging.conf")
user_offline_logger = logging.getLogger("user_offline")


def get_url_parmars(date_str, date_type):
    """
    :return: 返回所有参数的组合情况，生成url参数组合
    """
    # "app" 已废弃，产品说不需要展示，所以暂时不测试
    view = ["trend", "core_index", "customer", "platform", "bd"]
    source = [1, 2, 3, 4, 'all']
    parent_platform = [1, 2, 3, 4, 'all']
    # platform = ['all',1, 2, 3, 4, 5, 6, 7, 8, 9]
    platform = ['all', 1, 2, 3, 4, 5, 7, 8, 9]
    bd_id = [1, 2, 6, 4, 'all']
    shop_type = ''
    eliminate_type = ''
    sale_type = ''
    date_type = date_type
    # 'register_number',
    # field_str = ['daycount_ratio_sd',  'daycount_ratio_zf',' ','new_create_parent_uv_sd','new_create_parent_uv_zf',
    #              'new_create_parent_uv_ck','create_parent_uv_sd','create_parent_uv_zf','create_parent_uv_ck']
    field_str = [' ', 'daycount_ratio_sd', 'create_parent_uv_sd', 'create_parent_uv_zf', 'create_parent_uv_ck']
    date = date_str
    url_params = []
    for v in view:
        for s in source:
            for p_platform in parent_platform:
                for p in platform:
                    for bd in bd_id:
                        for f in field_str:
                            if s == 2 and (p_platform != 'all' or p != 'all' or bd_id != 'all'):
                                continue
                            elif s == 3 and (p_platform != 'all' or p != 'all' or bd_id != 'all'):
                                continue
                            elif s == 4 and (p_platform != 'all' or p != 'all' or bd_id != 'all'):
                                continue
                            elif s == 'all' and (p_platform != 'all' or p != 'all'):
                                continue
                            elif s == 1 and p_platform == 1 and p not in (1, 2, 'all'):
                                continue
                            elif s == 1 and p_platform == 2 and p not in (3, 4, 5, 6, 7, 8, 9, 'all'):
                                continue
                            elif s == 1 and p_platform == 3 and p != 'all':
                                continue
                            elif s == 1 and p_platform == 4 and p != 'all':
                                continue
                            elif s == 1 and p_platform == 'all' and p != 'all':
                                continue
                            elif v == 'core_index' and f != ' ':
                                continue
                            elif v != 'core_index' and f == ' ':
                                continue
                            elif v == 'customer' and f in (
                            'new_create_parent_uv_sd', 'new_create_parent_uv_zf', 'new_create_parent_uv_ck'):
                                continue
                            elif v != 'trend' and bd != 'all':
                                continue
                            else:
                                list_t = [v, s, p_platform, p, bd, shop_type, eliminate_type, sale_type, date_type,
                                          date, f]
                                url_params.append(list_t)
    return url_params


# 根据不同的页面拼接请求url的参数
def get_api_url(url_param_list):
    """
    返回接口查询使用参数，提供给数据库查询sql做条件
    :param
    :return: if语句中过滤掉了不存在的查询条件，切根据是否首页进行传参
    """
    url_list = []
    for params in url_param_list:
        view = params[0]
        source = params[1]
        parent_platform = params[2]
        platform = params[3]
        bd_id = params[4]
        shop_type = params[5]
        eliminate_type = params[6]
        sale_type = params[7]
        date_type = params[8]
        date = params[9]
        field_str = params[10]
        if date_type == 'd':
            core_index_date = date
        elif date_type == 'w':
            core_index_date = get_week_order(date)
        elif date_type == 'm':
            core_index_date = get_month_order(date)
        elif date_type == 'q':
            core_index_date = get_quarter_order(date)
        if view == 'core_index':
            url = """
             http://192.168.105.85:8085/api/overview_v5?action=user_analysis
             &view=%s
             &date_type=%s
             &date=%s
             &shop_type=%s
             &eliminate_type=%s
             &sale_type=%s
             &bd_id=%s
             &source=%s
             &parent_platform=%s
             &platform=%s
             """ % (
            view, date_type, core_index_date, shop_type, eliminate_type, sale_type, bd_id, source, parent_platform,
            platform)
        else:
            url = """
             http://192.168.105.85:8085/api/overview_v5?action=user_analysis
             &view=%s
             &field_str=%s              
             &date_type=%s
             &date=%s
             &shop_type=%s
             &eliminate_type=%s
             &source=%s
             &parent_platform=%s
             &platform=%s
             &sale_type=%s
             &bd_id=%s
             """ % (view, field_str, date_type, date, shop_type, eliminate_type, source, parent_platform,
                    platform, sale_type, bd_id)
        url = url.replace('\n', '').replace(' ', '')
        url_list.append(url)
    return url_list


def do_compare_index_data(params, api_data, ck_data, flag):
    source = params["source"]
    parent_platform = params["parent_platform"]
    platform = params["platform"]
    bd_id = params["bd_id"]
    date = params["date"]
    if len(api_data) > 0 and len(ck_data) > 0:
        for i in range(len(api_data)):
            for c in ck_data:
                for a in api_data[i]:
                    if c == a:
                        if ck_data[c] == api_data[i][a]:
                            user_offline_logger.info("==pass==%s:日期:%s,来源：%s,一级平台:%s,二级平台:%s,计算指标:%s" %
                                                     (flag, date, source, parent_platform, platform, c))
                        else:
                            user_offline_logger.info("==fail==%s:日期:%s,来源：%s,一级平台:%s,二级平台:%s,计算指标:%s" %
                                                     (flag, date, source, parent_platform, platform, c))
                            user_offline_logger.info("数据库:%s,api:%s" % (ck_data[c], api_data[i][a]))
                    else:
                        continue
    else:
        if len(api_data) != len(ck_data):
            user_offline_logger.info("===数据为空===%s:日期:%s,来源：%s,一级平台:%s,二级平台:%s" %
                                     (flag, date, source, parent_platform, platform))
            user_offline_logger.info("数据库:%s" % (ck_data))
            user_offline_logger.info("api:%s" % (api_data))


def do_compare_trip_data(params, api_result, ck_data, flag):
    """ 下钻页数据比对"""
    print("api:", api_result)
    print("database:", ck_data)
    source = params['source']
    parent_platform = params["parent_platform"]
    platform = params["platform"]
    bd_id = params["bd_id"]
    date = params["date"]
    field_str = params["field_str"]
    if len(api_result) > 0 and len(ck_data) > 0:
        for c in ck_data:
            for a in api_result:
                if c == a:
                    if ck_data[c] == api_result[a]:
                        user_offline_logger.info("==pass==%s:日期:%s,来源：%s,一级平台:%s,二级平台:%s,部门:%s,下钻页:%s,验证指标:%s" %
                                                 (flag, date, source, parent_platform, platform, bd_id, field_str, c))
                    else:
                        user_offline_logger.info("==fail==%s:日期:%s,来源：%s一级平台:%s,二级平台:%s,部门:%s,下钻页:%s,验证指标:%s" %
                                                 (flag, date, source, parent_platform, platform, bd_id, field_str, c))
                        user_offline_logger.info("数据库:%s,api:%s" % (ck_data[c], api_result[a]))
                else:
                    continue
    else:
        if len(api_result) != len(ck_data):
            user_offline_logger.info("==api数据为空==%s:日期:%s,来源：%s,一级平台:%s,二级平台:%s,部门:%s,下钻页：%s" %
                                     (flag, date, source, parent_platform, platform, bd_id, field_str))
            user_offline_logger.info("数据库:%s" % (ck_data))
            user_offline_logger.info("api:%s" % (api_result))


def do_compare_job_day():
    # 要测试数据的日期
    date_str = '2020-12-30'
    date_type = 'd'
    trend_start = get_end_time(date_str, -6)
    y_start = get_end_time(date_str, -1)
    tb_week_date = get_end_time(date_str, -7)
    tb_year_date = get_last_year_date(date_str)
    param = get_url_parmars(date_str, date_type)
    url_list = get_api_url(param)
    user_offline_logger.info("=======指标时间维度:%s==========" % date_type)
    #debug
    url_list=['http://192.168.105.85:8085/api/overview_v5?action=user_analysis&view=core_index&date_type=d&date=2020-12-30&shop_type=&eliminate_type=&sale_type=&bd_id=all&source=1&parent_platform=1&platform=all']
    #url_list=['http://192.168.105.85:8085/api/overview_v5?action=user_analysis&view=trend&field_str=daycount_ratio_sd&
    # date_type=d&date=2020-12-30&shop_type=&eliminate_type=&source=1&parent_platform=1&platform=all&sale_type=&bd_id=1']
    print(url_list)
    for url in url_list:
        print(url)
        params = parse_url_parma(url)
        date = date_str
        source = get_source(params)
        platform = get_sql_from_platform(params)
        bd_id = get_sql_bd_id(params)
        js_result = get_api_result(url)
        if 'view=core_index' in url:
            api_data, api_hb_data, api_tb_year, api_tb_week = parse_core_index_data(js_result)

            ck_result = get_index_result(date, date, source, platform)
            do_compare_index_data(params, api_data, ck_result, '日-首页数据')
            ck_hb_data = get_index_ratio_result(date, date, y_start, y_start, source, platform)
            do_compare_index_data(params, api_hb_data, ck_hb_data, '日-首页环比数据')
            ck_tb_week = get_index_ratio_result(date, date, tb_week_date, tb_week_date, source, platform)
            do_compare_index_data(params, api_tb_week, ck_tb_week, '日-首页上周同比数据')
            ck_tb_year = get_index_ratio_result(date, date, tb_year_date, tb_year_date, source, platform)
            do_compare_index_data(params, api_tb_year, ck_tb_year, '日-首页去年同比数据')
        else:
            field_str = params['field_str']
            # 因为trend接口返回json串的字典表中key为values,其他接口返回的key是value,所以需要单独处理
            if 'view=trend' in url:
                api_today_data = parse_trip_data_trend(js_result, date_type)
                # trend 下钻页要返回近7天的数据，所以天的开始时间要给近7天的开始日期
                sql_result = get_trend_result(trend_start, date, platform, source, bd_id, field_str, date_type)
                do_compare_trip_data(params, api_today_data, sql_result, '趋势')
            elif 'view=bd' in url:
                api_result, api_hb_map, api_tb_year_map, api_tb_week_map, api_zb_map = parse_trip_data(js_result)
                ck_result = get_bd_result(date, date, platform, source, bd_id, field_str, date_type)
                if len(api_result) != 0 and len(ck_result) != 0:
                    do_compare_trip_data(params, api_result, ck_result, "事业部分布")
                ck_hb_result = get_bd_result(y_start, y_start, platform, source, bd_id, field_str, date_type)
                ck_hb_ratio = get_trip_hb_result(ck_hb_result, ck_result)
                if len(ck_hb_ratio) > 0:
                    do_compare_trip_data(params, api_hb_map, ck_hb_ratio, "事业部分布-环比")
                ck_tb_week = get_bd_result(tb_week_date, tb_week_date, platform, source, bd_id, field_str, date_type)
                ck_tb_week_ratio = get_trip_hb_result(ck_tb_week, ck_result)
                if len(ck_tb_week_ratio) > 0:
                    do_compare_trip_data(params, api_tb_week_map, ck_tb_week_ratio, "事业部分布-同比上周")
                ck_tb_year = get_bd_result(tb_year_date, tb_year_date, platform, source, bd_id, field_str, date_type)
                ck_tb_year_ratio = get_trip_hb_result(ck_tb_year, ck_result)
                if len(ck_tb_year_ratio) > 0:
                    do_compare_trip_data(params, api_tb_year_map, ck_tb_year_ratio, "事业部分布-同比去年")
                ck_zb_data = get_zb_result(date, date, platform, source, field_str, ck_result, date_type)
                if len(api_zb_map) != 0 or len(ck_zb_data) != 0:
                    do_compare_trip_data(params, api_zb_map, ck_zb_data, "事业部分布-占比")
            elif 'view=customer' in url:
                if field_str not in ('new_create_parent_uv_sd', 'new_create_parent_uv_zf', 'new_create_parent_uv_ck'):
                    api_result, api_hb_map, api_tb_year_map, api_tb_week_map, api_zb_map = parse_trip_data(js_result)
                    ck_result = get_customer_result(date, date, platform, source, field_str, date_type)
                    if len(api_result) != 0 and len(ck_result) != 0:
                        do_compare_trip_data(params, api_result, ck_result, "新老客分布")
                    ck_hb_result = get_customer_result(y_start, y_start, platform, source, field_str, date_type)
                    ck_hb_ratio = get_trip_hb_result(ck_hb_result, ck_result)
                    if len(ck_hb_ratio) > 0:
                        do_compare_trip_data(params, api_hb_map, ck_hb_ratio, "新老客分布-环比")
                    ck_tb_week = get_customer_result(tb_week_date, tb_week_date, platform, source, field_str, date_type)
                    ck_tb_week_ratio = get_trip_hb_result(ck_tb_week, ck_result)
                    if len(ck_tb_week_ratio) > 0:
                        do_compare_trip_data(params, api_tb_week_map, ck_tb_week_ratio, "新老客分布-同比上周")
                    ck_tb_year = get_customer_result(tb_year_date, tb_year_date, platform, source, field_str, date_type)
                    ck_tb_year_ratio = get_trip_hb_result(ck_tb_year, ck_result)
                    if len(ck_tb_year_ratio) > 0:
                        do_compare_trip_data(params, api_tb_year_map, ck_tb_year_ratio, "新老客分布-同比去年")
                    ck_zb_data = get_zb_result(date, date, platform, source, field_str, ck_result, date_type)
                    if len(api_zb_map) != 0 or len(ck_zb_data) != 0:
                        do_compare_trip_data(params, api_zb_map, ck_zb_data, "新老客分布-占比")
                else:
                    continue
            elif 'view=platform' in url:
                if source == '(1,2,3,4)' or platform == '(1,2)' or platform == '(1,2,3,4,5,6,7,8,9,12,20,0)':
                    api_result, api_hb_map, api_tb_year_map, api_tb_week_map, api_zb_map = parse_trip_data(js_result)
                    ck_result = get_platform_result(date, date, platform, source, field_str, date_type)
                    if len(api_result) != 0 and len(ck_result) != 0:
                        do_compare_trip_data(params, api_result, ck_result, "平台分布")
                    ck_hb_result = get_platform_result(y_start, y_start, platform, source, field_str, date_type)
                    ck_hb_ratio = get_trip_hb_result(ck_hb_result, ck_result)
                    if len(ck_hb_ratio) > 0:
                        do_compare_trip_data(params, api_hb_map, ck_hb_ratio, "平台分布-环比")
                    ck_tb_week = get_platform_result(tb_week_date, tb_week_date, platform, source, field_str, date_type)
                    ck_tb_week_ratio = get_trip_hb_result(ck_tb_week, ck_result)
                    if len(ck_tb_week_ratio) > 0:
                        do_compare_trip_data(params, api_tb_week_map, ck_tb_week_ratio, "平台分布-同比上周")
                    ck_tb_year = get_platform_result(tb_year_date, tb_year_date, platform, source, field_str, date_type)
                    ck_tb_year_ratio = get_trip_hb_result(ck_tb_year, ck_result)
                    if len(ck_tb_year_ratio) > 0:
                        do_compare_trip_data(params, api_tb_year_map, ck_tb_year_ratio, "平台分布-同比去年")

                    ck_zb_data = get_zb_result(date, date, platform, source, field_str, ck_result, date_type)
                    if len(api_zb_map) != 0 or len(ck_zb_data) != 0:
                        do_compare_trip_data(params, api_zb_map, ck_zb_data, "平台分布-占比")
                else:
                    continue


def do_compare_job_week():
    # 要测试数据的日期 选择一个是周日的日期，方便调用函数get_sql_week()中计算一整周的时间
    date_str = '2020-12-27'
    start, end = get_sql_week(date_str)
    last_week_list = get_weekdays_by_num(date_str, 2)
    last_week_start = last_week_list[1][0]
    last_week_end = last_week_list[1][1]
    date_type = 'w'
    user_offline_logger.info("=======指标时间维度:%s==========" % date_type)
    # 通过date_type来选择传入url参数中的日期类型
    param = get_url_parmars(date_str, date_type)
    url_list = get_api_url(param)
    for url in url_list:
        print(url)
        params = parse_url_parma(url)
        source = get_source(params)
        platform = get_sql_from_platform(params)
        bd_id = get_sql_bd_id(params)
        js_result = get_api_result(url)
        if 'view=core_index' in url:
            api_data, api_hb_data, api_tb_year, api_tb_week = parse_core_index_data(js_result)
            ck_result = get_index_result(start, end, source, platform)
            ck_hb_data = get_index_ratio_result(start, end, last_week_start, last_week_end, source, platform)
            do_compare_index_data(params, api_data, ck_result, '周-首页数据')
            do_compare_index_data(params, api_hb_data, ck_hb_data, '周-首页环比数据')
        else:
            field_str = params['field_str']
            # 因为trend接口返回json串的字典表中key为values,其他接口返回的key是value,所以需要单独处理
            if 'view=trend' in url:
                api_today_data = parse_trip_data_trend(js_result, date_type)
                sql_result = get_trend_result(start, end, platform, source, bd_id, field_str, date_type)
                do_compare_trip_data(params, api_today_data, sql_result, '趋势')
            elif 'view=bd' in url:
                api_result, api_hb_map, api_tb_year_map, api_tb_week_map, api_zb_map = parse_trip_data(js_result)
                ck_result = get_bd_result(start, end, platform, source, bd_id, field_str, date_type)
                if len(api_result) != 0 and len(ck_result) != 0:
                    do_compare_trip_data(params, api_result, ck_result, "事业部分布")
                ck_hb_result = get_bd_result(last_week_start, last_week_end, platform, source, bd_id, field_str,
                                             date_type)
                ck_hb_ratio = get_trip_hb_result(ck_hb_result, ck_result)
                if len(ck_hb_ratio) > 0:
                    do_compare_trip_data(params, api_hb_map, ck_hb_ratio, "事业部分布-环比")
                ck_zb_data = get_zb_result(start, end, platform, source, field_str, ck_result, date_type)
                if len(api_zb_map) != 0 or len(ck_zb_data) != 0:
                    do_compare_trip_data(params, api_zb_map, ck_zb_data, "事业部分布-占比")
            elif 'view=customer' in url:
                if field_str not in ('new_create_parent_uv_sd', 'new_create_parent_uv_zf', 'new_create_parent_uv_ck'):
                    api_result, api_hb_map, api_tb_year_map, api_tb_week_map, api_zb_map = parse_trip_data(js_result)
                    ck_result = get_customer_result(start, end, platform, source, field_str, date_type)
                    ck_hb_result = get_customer_result(last_week_start, last_week_end, platform, source, field_str,
                                                       date_type)
                    ck_hb_ratio = get_trip_hb_result(ck_hb_result, ck_result)
                    if len(ck_hb_ratio) > 0:
                        do_compare_trip_data(params, api_hb_map, ck_hb_ratio, "新老客分布-环比")
                    ck_zb_data = get_zb_result(start, end, platform, source, field_str, ck_result, date_type)
                    if len(api_zb_map) != 0 or len(ck_zb_data) != 0:
                        do_compare_trip_data(params, api_zb_map, ck_zb_data, "新老客分布-占比")
                else:
                    continue
            elif 'view=platform' in url:
                if source == '(1,2,3,4)' or platform == '(1,2)' or platform == '(1,2,3,4,5,6,7,8,9,12,20,0)':
                    api_result, api_hb_map, api_tb_year_map, api_tb_week_map, api_zb_map = parse_trip_data(js_result)
                    ck_result = get_platform_result(start, end, platform, source, field_str, date_type)
                    ck_hb_result = get_platform_result(last_week_start, last_week_end, platform, source, field_str,
                                                       date_type)

                    ck_hb_ratio = get_trip_hb_result(ck_hb_result, ck_result)
                    if len(ck_hb_ratio) > 0:
                        do_compare_trip_data(params, api_hb_map, ck_hb_ratio, "平台分布-环比")
                    ck_zb_data = get_zb_result(start, end, platform, source, field_str, ck_result, date_type)

                    if len(api_zb_map) != 0 or len(ck_zb_data) != 0:
                        do_compare_trip_data(params, api_zb_map, ck_zb_data, "平台分布-占比")
                else:
                    continue


def do_compare_job_month():
    # 要测试数据的日期 选择一个是周日的日期，方便调用函数get_sql_week()中计算一整周的时间
    date_str = '2020-12-27'
    last_month_list = get_monthdays_by_num(date_str, 2)
    start = last_month_list[0][0]
    end = last_month_list[0][1]
    last_month_start = last_month_list[1][0]
    last_month_end = last_month_list[1][1]
    last_year_day = get_last_year_date(date_str)
    last_year_month_list = get_monthdays_by_num(last_year_day, 1)
    tb_year_start = last_year_month_list[0][0]
    tb_year_end = last_year_month_list[0][1]
    date_type = 'm'
    user_offline_logger.info("=======指标时间维度:%s==========" % date_type)
    # 通过date_type来选择传入url参数中的日期类型
    param = get_url_parmars(date_str, date_type)
    url_list = get_api_url(param)
    for url in url_list:
        print(url)
        params = parse_url_parma(url)
        source = get_source(params)
        platform = get_sql_from_platform(params)
        bd_id = get_sql_bd_id(params)
        js_result = get_api_result(url)
        if 'view=core_index' in url:
            api_data, api_hb_data, api_tb_year, api_tb_week = parse_core_index_data(js_result)
            ck_result = get_index_result(start, end, source, platform)
            ck_hb_data = get_index_ratio_result(start, end, last_month_start, last_month_end, source, platform)
            do_compare_index_data(params, api_data, ck_result, '月-首页数据')
            do_compare_index_data(params, api_hb_data, ck_hb_data, '月-首页环比数据')
            ck_tb_year = get_index_ratio_result(start, end, tb_year_start, tb_year_end, source, platform)
            do_compare_index_data(params, api_tb_year, ck_tb_year, '月-首页去年同比数据')
        else:
            field_str = params['field_str']
            # 因为trend接口返回json串的字典表中key为values,其他接口返回的key是value,所以需要单独处理
            if 'view=trend' in url:
                api_today_data = parse_trip_data_trend(js_result, date_type)
                sql_result = get_trend_result(start, end, platform, source, bd_id, field_str, date_type)
                do_compare_trip_data(params, api_today_data, sql_result, '趋势')
            elif 'view=bd' in url:
                api_result, api_hb_map, api_tb_year_map, api_tb_week_map, api_zb_map = parse_trip_data(js_result)
                ck_result = get_bd_result(start, end, platform, source, bd_id, field_str, date_type)
                if len(api_result) != 0 and len(ck_result) != 0:
                    do_compare_trip_data(params, api_result, ck_result, "事业部分布")
                ck_hb_result = get_bd_result(last_month_start, last_month_end, platform, source, bd_id, field_str,
                                             date_type)
                ck_hb_ratio = get_trip_hb_result(ck_hb_result, ck_result)
                if len(ck_hb_ratio) > 0:
                    do_compare_trip_data(params, api_hb_map, ck_hb_ratio, "事业部分布-环比")
                ck_tb_result = get_bd_result(tb_year_start, tb_year_end, platform, source, bd_id, field_str, date_type)
                ck_tb_ratio = get_trip_hb_result(ck_tb_result, ck_result)
                if len(ck_tb_ratio) > 0:
                    do_compare_trip_data(params, api_tb_year_map, ck_tb_ratio, "事业部分布-同比去年")
                ck_zb_data = get_zb_result(start, end, platform, source, field_str, ck_result, date_type)
                if len(api_zb_map) != 0 or len(ck_zb_data) != 0:
                    do_compare_trip_data(params, api_zb_map, ck_zb_data, "事业部分布-占比")
            elif 'view=customer' in url:
                if field_str not in ('new_create_parent_uv_sd', 'new_create_parent_uv_zf', 'new_create_parent_uv_ck'):
                    api_result, api_hb_map, api_tb_year_map, api_tb_week_map, api_zb_map = parse_trip_data(js_result)
                    ck_result = get_customer_result(start, end, platform, source, field_str, date_type)
                    if len(api_result) != 0 and len(ck_result) != 0:
                        do_compare_trip_data(params, api_result, ck_result, "新老客分布")
                    ck_hb_result = get_customer_result(last_month_start, last_month_end, platform, source,
                                                       field_str, date_type)
                    ck_hb_ratio = get_trip_hb_result(ck_hb_result, ck_result)
                    if len(ck_hb_ratio) > 0:
                        do_compare_trip_data(params, api_hb_map, ck_hb_ratio, "新老客分布-环比")
                    ck_tb_result = get_customer_result(tb_year_start, tb_year_end, platform, source, field_str,
                                                       date_type)
                    ck_tb_ratio = get_trip_hb_result(ck_tb_result, ck_result)
                    if len(ck_tb_ratio) > 0:
                        do_compare_trip_data(params, api_tb_year_map, ck_tb_ratio, "新老客分布-同比去年")
                    if field_str != 'daycount_ratio_sd' or field_str != 'daycount_ratio_zf':
                        ck_zb_data = get_zb_result(start, end, platform, source, field_str, ck_result, date_type)
                        if len(api_zb_map) != 0 or len(ck_zb_data) != 0:
                            do_compare_trip_data(params, api_zb_map, ck_zb_data, "新老客分布-占比")
                    else:
                        continue
                else:
                    continue
            elif 'view=platform' in url:
                if source == '(1,2,3,4)' or platform == '(1,2)' or platform == '(1,2,3,4,5,6,7,8,9,12,20,0)':
                    api_result, api_hb_map, api_tb_year_map, api_tb_week_map, api_zb_map = parse_trip_data(js_result)
                    ck_result = get_platform_result(start, end, platform, source, field_str, date_type)
                    if len(api_result) != 0 and len(ck_result) != 0:
                        do_compare_trip_data(params, api_result, ck_result, "平台分布")
                    ck_hb_result = get_platform_result(last_month_start, last_month_end, platform, source,
                                                       field_str, date_type)
                    ck_hb_ratio = get_trip_hb_result(ck_hb_result, ck_result)
                    if len(ck_hb_ratio) > 0:
                        do_compare_trip_data(params, api_hb_map, ck_hb_ratio, "平台分布-环比")
                    ck_tb_result = get_platform_result(tb_year_start, tb_year_end, platform, source,
                                                       field_str, date_type)
                    ck_tb_ratio = get_trip_hb_result(ck_tb_result, ck_result)
                    if len(ck_tb_ratio) > 0:
                        do_compare_trip_data(params, api_tb_year_map, ck_tb_ratio, "平台分布-同比去年")
                    ck_zb_data = get_zb_result(start, end, platform, source, field_str, ck_result, date_type)
                    if len(api_zb_map) != 0 or len(ck_zb_data) != 0:
                        do_compare_trip_data(params, api_zb_map, ck_zb_data, "平台分布-占比")
                else:
                    continue


def do_compare_job_quarter():
    # 要测试数据的日期 选择一个是周日的日期，方便调用函数get_sql_week()中计算一整周的时间
    date_str = '2020-12-27'
    last_quarter_list = get_quarterdays_by_num(date_str, 2)
    start = last_quarter_list[0][0]
    end = last_quarter_list[0][1]
    last_quarter_start = last_quarter_list[1][0]
    last_quarter_end = last_quarter_list[1][1]
    last_year_day = get_last_year_date(date_str)
    last_year_quarter_list = get_quarterdays_by_num(last_year_day, 1)
    tb_year_start = last_year_quarter_list[0][0]
    tb_year_end = last_year_quarter_list[0][1]
    date_type = 'q'
    user_offline_logger.info("=======指标时间维度:%s==========" % date_type)
    # 通过date_type来选择传入url参数中的日期类型
    param = get_url_parmars(date_str, date_type)
    url_list = get_api_url(param)
    for url in url_list:
        print(url)
        params = parse_url_parma(url)
        source = get_source(params)
        platform = get_sql_from_platform(params)
        bd_id = get_sql_bd_id(params)
        js_result = get_api_result(url)
        if 'view=core_index' in url:
            api_data, api_hb_data, api_tb_year, api_tb_week = parse_core_index_data(js_result)
            ck_result = get_index_result(start, end, source, platform)
            ck_hb_data = get_index_ratio_result(start, end, last_quarter_start, last_quarter_end, source, platform)
            do_compare_index_data(params, api_data, ck_result, '季-首页数据')
            do_compare_index_data(params, api_hb_data, ck_hb_data, '季-首页环比数据')
            ck_tb_year = get_index_ratio_result(start, end, tb_year_start, tb_year_end, source, platform)
            do_compare_index_data(params, api_tb_year, ck_tb_year, '季-首页去年同比数据')
        else:
            field_str = params['field_str']
            # 因为trend接口返回json串的字典表中key为values,其他接口返回的key是value,所以需要单独处理
            if 'view=trend' in url:
                api_today_data = parse_trip_data_trend(js_result, date_type)
                sql_result = get_trend_result(start, end, platform, source, bd_id, field_str, date_type)
                do_compare_trip_data(params, api_today_data, sql_result, '趋势')
            elif 'view=bd' in url:
                api_result, api_hb_map, api_tb_year_map, api_tb_week_map, api_zb_map = parse_trip_data(js_result)
                ck_result = get_bd_result(start, end, platform, source, bd_id, field_str, date_type)
                if len(api_result) != 0 and len(ck_result) != 0:
                    do_compare_trip_data(params, api_result, ck_result, "事业部分布")
                ck_hb_result = get_bd_result(last_quarter_start, last_quarter_end, platform, source, bd_id, field_str,
                                             date_type)
                ck_hb_ratio = get_trip_hb_result(ck_hb_result, ck_result)
                if len(ck_hb_ratio) > 0:
                    do_compare_trip_data(params, api_hb_map, ck_hb_ratio, "事业部分布-环比")
                ck_tb_result = get_bd_result(tb_year_start, tb_year_end, platform, source, bd_id, field_str, date_type)
                ck_tb_ratio = get_trip_hb_result(ck_tb_result, ck_result)
                if len(ck_tb_ratio) > 0:
                    do_compare_trip_data(params, api_tb_year_map, ck_tb_ratio, "事业部分布-同比去年")
                ck_zb_data = get_zb_result(start, end, platform, source, field_str, ck_result, date_type)
                if len(api_zb_map) != 0 or len(ck_zb_data) != 0:
                    do_compare_trip_data(params, api_zb_map, ck_zb_data, "事业部分布-占比")

            elif 'view=customer' in url:
                if field_str not in ('new_create_parent_uv_sd', 'new_create_parent_uv_zf', 'new_create_parent_uv_ck'):
                    api_result, api_hb_map, api_tb_year_map, api_tb_week_map, api_zb_map = parse_trip_data(js_result)
                    ck_result = get_customer_result(start, end, platform, source, field_str, date_type)
                    if len(api_result) != 0 and len(ck_result) != 0:
                        do_compare_trip_data(params, api_result, ck_result, "新老客分布")
                    ck_hb_result = get_customer_result(last_quarter_start, last_quarter_end, platform, source,
                                                       field_str, date_type)
                    ck_hb_ratio = get_trip_hb_result(ck_hb_result, ck_result)
                    if len(ck_hb_ratio) > 0:
                        do_compare_trip_data(params, api_hb_map, ck_hb_ratio, "新老客分布-环比")
                    ck_tb_result = get_customer_result(tb_year_start, tb_year_end, platform, source, field_str,
                                                       date_type)
                    ck_tb_ratio = get_trip_hb_result(ck_tb_result, ck_result)
                    if len(ck_tb_ratio) > 0:
                        do_compare_trip_data(params, api_tb_year_map, ck_tb_ratio, "新老客分布-同比去年")
                    ck_zb_data = get_zb_result(start, end, platform, source, field_str, ck_result, date_type)
                    if len(api_zb_map) != 0 or len(ck_zb_data) != 0:
                        do_compare_trip_data(params, api_zb_map, ck_zb_data, "新老客分布-占比")
                else:
                    continue
            elif 'view=platform' in url:
                if source == '(1,2,3,4)' or platform == '(1,2)' or platform == '(1,2,3,4,5,6,7,8,9,12,20,0)':
                    api_result, api_hb_map, api_tb_year_map, api_tb_week_map, api_zb_map = parse_trip_data(js_result)
                    ck_result = get_platform_result(start, end, platform, source, field_str, date_type)
                    if len(api_result) != 0 and len(ck_result) != 0:
                        do_compare_trip_data(params, api_result, ck_result, "平台分布")
                    ck_hb_result = get_platform_result(last_quarter_start, last_quarter_end, platform, source,
                                                       field_str, date_type)
                    ck_hb_ratio = get_trip_hb_result(ck_hb_result, ck_result)
                    if len(ck_hb_ratio) > 0:
                        do_compare_trip_data(params, api_hb_map, ck_hb_ratio, "平台分布-环比")
                    ck_tb_result = get_platform_result(tb_year_start, tb_year_end, platform, source,
                                                       field_str, date_type)
                    ck_tb_ratio = get_trip_hb_result(ck_tb_result, ck_result)
                    if len(ck_tb_ratio) > 0:
                        do_compare_trip_data(params, api_tb_year_map, ck_tb_ratio, "平台分布-同比去年")
                    ck_zb_data = get_zb_result(start, end, platform, source, field_str, ck_result, date_type)
                    if len(api_zb_map) != 0 or len(ck_zb_data) != 0:
                        do_compare_trip_data(params, api_zb_map, ck_zb_data, "平台分布-占比")
                else:
                    continue


def test():
    date_str = '2020-12-27'
    start, end = get_sql_week(date_str)
    last_week_list = get_weekdays_by_num(date_str, 2)
    last_week_start = last_week_list[1][0]
    last_week_end = last_week_list[1][1]
    date_type = 'd'
    user_offline_logger.info("=======指标时间维度:%s==========" % date_type)
    url = "http://192.168.105.85:8085/api/overview_v5?action=user_analysis&view=trend&field_str=create_parent_uv_sd&date_type=d&date=2020-12-30&shop_type=&eliminate_type=&source=1&parent_platform=1&platform=1&sale_type=&bd_id=2"

    js_result = get_api_result(url)
    api_result = parse_trip_data_trend(js_result, date_type)
    print(api_result)
    for key, value in api_result.items():
        print(type(key), key, type(value), value)


def run_job():
    do_compare_job_day()
    # do_compare_job_week()
    # do_compare_job_month()
    # do_compare_job_quarter()
    # test()





