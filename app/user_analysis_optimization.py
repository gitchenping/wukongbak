from utils import util,tb_hb,decorate

from db.dao.user_analysis import sql_user_analysis_drill_op,sql_user_analysis_overview_op
from api.service.user_analysis import api_user_analysis_drill_op,api_user_analysis_overview_op
from resources.map import user_dict
import logging.config
from os import path



#日志设置
filepath=path.join(path.dirname(path.dirname(__file__)),"conf","logging.conf")
logging.config.fileConfig(filepath)
userlogger=logging.getLogger('user')

#url
filepath=path.join(path.dirname(path.dirname(__file__)),"conf","config.ini")
cf=util.readini(filepath)
url=cf.get('api','user_analysis_api')

#ck 连接 、名称
# ck_conn=util.connect_clickhouse()
ck_db={
    'host':"http://10.0.5.80:8123",
    'headers':{'X-ClickHouse-User': 'membersbi', 'X-ClickHouse-Key': 'dangdangbi'}
}

#各指标对应的计算库表
'''
test_indicator_dict={"new_uv":"新访UV" ,#'mdata_flows_user_realtime_day_all',
           "new_uv_ratio":"新访UV占比",  #'mdata_flows_user_realtime_day_all'
           "register_number":"新增注册用户" ,    #'mdata_customer_new_all'
           "new_create_parent_uv_sd":"新增收订用户",  #'dm_order_create_day' 
           "new_create_parent_uv_zf":"新增支付用户" ,             #'dm_order_pay_day'
           "new_create_parent_uv_ck": "新增出库用户",             #'dm_order_send_day'
           "uv":"活跃UV" ,        #'mdata_flows_user_realtime_day_all'
           "create_parent_uv_sd":"收订用户",    #'dm_order_create_day'
           "create_parent_uv_zf":"支付用户",    #'dm_order_pay_day'
           "create_parent_uv_ck":"出库用户",    #'dm_order_send_day'
           "daycount_ratio_sd":"收订下单频次",    #'dm_order_create_day'
           "daycount_ratio_zf":"支付下单频次"    #'dm_order_pay_day'
           }
'''

#测试指标（tab模块）
test_indicator_dict = dict(user_dict)

@decorate.complog(userlogger)
def user_drill(data,indicator_name,indicator):
    datacopy=dict(data)

    if indicator=='register_number' and (data['bd_id'] != 'all' or data['shop_type'] != 'all'):
        return

    apidata = api_user_analysis_drill_op(url,datacopy,indicator_name)
    sqldata = sql_user_analysis_drill_op(datacopy,ck_db,indicator,indicator_name)

    diff = -1
    try:
        diff = util.diff_dict(apidata, sqldata)
    except Exception as e:
        print(e.__repr__())
        print(data)

    return diff


    pass


@decorate.complog(userlogger)
def user_overview(data):
    datacopy = dict(data)

    apidata = api_user_analysis_overview_op(url,datacopy, test_indicator_dict)
    sqldata = sql_user_analysis_overview_op(datacopy,test_indicator_dict,ck_db)

    diff=-1
    try:
        diff= util.diff_dict(apidata,sqldata)
    except Exception as e:
        print(e.__repr__())
        print(data)

    return diff

    pass


def user_analysis_op(datetype,date_str):

    for source in ['all','1','2','3','4']:                 #平台来源      all-all 1-主站 2-天猫 3-抖音 4-拼多多
        if source!='1':
            parent_platformlist = ['all']
        else :                                               #点击主站可以下钻 all-全部、1-APP、2-轻应用、3-H5、4-PC
            parent_platformlist=[ 'all','1', '2', '3','4']
        for parent_platform in parent_platformlist:
            if parent_platform == '1':
                platformlist = ['all','1', '2']                      # 1-android 、 2-ios
            elif parent_platform == '2':
                platformlist = ['3', '4','5','6','7','8','9']       # all、3-快应用、4-微信小程序、5-百度小程序、6-头条、7-支付宝、8-qq小程序、9-360小程序
            else:
                platformlist=['all']

            for platform in platformlist:

                for bd_id in ['all']:         #['all', '2', '4','5','6','1']:            # 事业部id：all-全部 1-出版物事业部 2-日百 3-数字业务事业部 4-文创 5-其它 6-服装

                    for shop_type in ['all', '1', '2']:                 # 经营方式 all-ALL 1-自营 2-招商

                        for eliminate_type in ['all', '1','2','3','4','5','99']:    # 剔除选项 all-不剔除 1-剔除建工 2-剔除大单 3-剔除风险 4-剔除企销 5-剔除实体分销 6-剔除全部

                            data={'source': source,
                                  'platform': platform,
                                  'parent_platform': parent_platform,
                                  'bd_id':bd_id,
                                  'shop_type':shop_type,
                                  'eliminate_type':eliminate_type,
                                  'date_type':datetype,
                                  'date': date_str
                                  }

                            # data={'source': '1', 'platform': 'all', 'parent_platform': 'all', 'bd_id': '1', 'shop_type': 'all',
                            #       'eliminate_type': 'all', 'date_type': 'd', 'date': '2021-05-19'}

                            # user_overview(data)
                            for indicator,indicator_name in test_indicator_dict.items():


                                data['field_str']=indicator
                                # data={'source': 'all', 'platform': 'all', 'parent_platform': 'all', 'bd_id': 'all',
                                #  'shop_type': 'all', 'eliminate_type': '2', 'date_type': 'd', 'date': '2021-05-19',
                                #  'field_str': 'create_parent_uv_sd'}
                                user_drill(data,test_indicator_dict[data['field_str']],data['field_str'])

def run_job():
    '''用户分析'''
    date_str='2021-05-19'

    date_type = ['day','mtd','qtd','wtd']           # 日、周、月、季度

    for datetype in date_type:

        user_analysis_op(datetype[0],date_str)