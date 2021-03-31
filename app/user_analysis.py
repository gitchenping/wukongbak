from utils import util,decorate
import logging.config
from . import api
from . import sql
import datetime


logging.config.fileConfig("logging.conf")
userlogger=logging.getLogger('user')

ck_tables = 'bi_mdata.kpi_order_info_all_v2'
data_type_dict={'3':'出库用户','2':'支付用户','1':'收订用户'}

zhibiao_dict = {'create_parent_uv_sd': '收订用户', 'create_parent_uv_zf': '支付用户', 'create_parent_uv_ck': '出库用户'}

@decorate.complog(userlogger)
def user_drill(data):
    datacopy=dict(data)

    apidata = api.api_user_analysis_drill(datacopy, zhibiao_dict)
    sqldata = sql.sql_user_analysis_drill(datacopy, ck_tables,zhibiao_dict, data_type_dict)
    util.diff_dict(sqldata, apidata)
    pass

@decorate.complog(userlogger)
def user_overview(data):
    datacopy = dict(data)

    sqldata = sql.sql_user_analysis_overview(datacopy, ck_tables, data_type_dict)
    apidata = api.api_user_analysis_overview(datacopy, zhibiao_dict)
    util.diff_dict(sqldata,apidata)
    pass


def run_job():
    datetimenow=datetime.datetime.now()
    year=str(datetimenow.year)
    month=str(datetimenow.month)
    day=str(datetimenow.day)
    hour=str(datetimenow.hour-1)

    date_str=year+"-"+(len(month)<2 and '0'+month or month) +"-"+(len(day)<2 and '0'+day or day)
    hour_str=len(hour)<2 and '0'+hour or hour


    for source in ['all','1','3','2','4']:                 #平台来源      all-all 1主站 2天猫 3抖音 4拼多多
        if source!='1':
            parent_platformlist = ['all']
        else :                                               #点击主站可以下钻APP、轻应用、H5、PC、其他
            parent_platformlist=[ 'all','1', '2', '3','4']
        for parent_platform in parent_platformlist:
            if parent_platform == '1':
                platformlist = ['all','1', '2']                      # android 、 ios
            elif parent_platform == '2':
                platformlist = ['3', '4','5','6','7','8','9'] # all、快应用、微信小程序、百度小程序、头条、qq、360
            else:
                platformlist=['all']
            for platform in platformlist:

                data={'source': source, 'platform': platform,'parent_platform': parent_platform,
                      'date_type':'h','date_str': date_str+" "+hour_str}

                # data={'source': '1', 'platform': '5', 'parent_platform': '2', 'date_type': 'h', 'date_str': '2021-02-01 12'}

                user_overview(data)
                user_drill(data)
