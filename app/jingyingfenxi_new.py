import json
from utils import util
import logging
from . import api
from db.dao.jingyingfenxi import *
from api.service.user_analysis import *

conn_ck = util.connect_clickhouse()
#经营分析API
cf=util.readini('./config.ini')
jingying_analysis_api_path=cf.get('api','jingying_analysis_api')

#logger
logging.config.fileConfig("logging.conf")
userlogger=logging.getLogger('jingying_overview')


table_dict={ 'sd':'bi_mdata.dm_order_create_day',
             'zf':'bi_mdata.dm_order_pay_day',
             'ck':'bi_mdata.dm_order_send_day'
}

def jingyingfenxi_overview(jingying_analysis_api_path,data):
    apidata=api_jingyingfenxi_overview(jingying_analysis_api_path,data)
    sqldata=sql_jingyingfenxi_overview(data,table_dict)

    #diff
    userlogger.info('筛选条件为: '+ str(data))
    util.diff(apidata,sqldata,userlogger)
    userlogger.info(' ')

def jingyingfenxi_drill(jingying_analysis_api_path,data):
    # apidata=api.api_jingyingfenxi_drill(jingying_analysis_api_path,data)
    sqldata=sql_jingyingfenxi_drill(data,table_dict)


    userlogger.info('筛选条件为: ' + str(data))
    if apidata.__contains__('支付转化率'):
        apidata.pop('支付转化率')

    for key in apidata.keys():
        #diff
        userlogger.info(key+"下钻")
        util.diff(apidata[key], sqldata[key], userlogger)


def jingyingfenxi_new(date_type,date):
    '''根据接口传参，从ck计算指标'''
    for source in ['all','1','2', '3', '4']:  # 平台来源      all-all 1主站 2天猫 3抖音 4拼多多
        if source != '1':
            parent_platformlist = ['all']
        else:  # 点击主站可以下钻APP、轻应用、H5、PC
            parent_platformlist = ['2', '3', '4']

        for parent_platform in parent_platformlist:

            if parent_platform == '1':
                platformlist = ['1', '2']                                  # android 、 ios

            elif parent_platform == '2':
                platformlist = ['5','all', '3', '4',  '6', '7', '8', '9']  # all、3-快应用、4-微信小程序、5-百度小程序、6-头条、7-qq、8-360

            else :
                platformlist = ['all']

            for platform in platformlist:

                for bd_id in ['1','2','4','all']:  # 事业部id：all-all 1-出版物事业部 2-日百服 3-数字业务事业部 4-文创

                    for shop_type in ['all', '1', '2']:  # 经营方式 all-ALL 1-自营 2-招商

                        for eliminate_type in ['all', '1']:  # 剔除选项 all-all 1-剔除建工

                            for sale_type in ['zf']:  # 收订sd、支付zf、出库ck

                                data = {'source': source, 'parent_platform': parent_platform, 'platform': platform,
                                        'bd_id': bd_id, 'shop_type': shop_type, 'eliminate_type': eliminate_type,
                                        'sale_type': sale_type, \
                                        'date_type': date_type, 'date_str': date}
                                data={'source': 'all', 'parent_platform': 'all', 'platform': 'all', 'bd_id': '4', 'shop_type': '1', 'eliminate_type': 'all', 'sale_type': 'ck', 'date_type': 'mtd', 'date_str': '2021-01-14'}
                                print(data)
                                # if sale_type!='zf':
                                #     jingyingfenxi_overview(jingying_analysis_api_path,data,conn_ck)

                                jingyingfenxi_drill(jingying_analysis_api_path,data,conn_ck)


def jingyingfenxi():
    date = '2021-01-14'
    date_type = ['mtd','day','wtd','qtd',]                                 # 日、周、月、季度

    for datetype in date_type:

        jingyingfenxi_new(datetype,date)