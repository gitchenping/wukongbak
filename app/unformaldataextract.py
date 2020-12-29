import configparser
import logging
from util import util
from util import log



def readini(path):
    cf=configparser.ConfigParser()
    cf.read(path,encoding='utf-8')
    return cf

cf=readini('./config.ini')

url_host=cf.get('api','api_host')

#经营分析API
jingying_analysis_api_path=cf.get('api','jingying_analysis_api')
jingying_analysis_api_url=url_host+jingying_analysis_api_path

#用户分析API
user_analysis_api_path=cf.get('api','user_analysis_api')
user_analysis_api_url=url_host+user_analysis_api_path



def jingying_analysis(datetype,date):
    '''经营分析'''
    jingying_analysis_overview_api_url = jingying_analysis_api_url.format('core_index')
    jingying_analysis_drill_api_url = jingying_analysis_api_url


    for source in ['1','2','3','4']:                  #平台来源      all-all 1主站 2天猫 3抖音 4拼多多
        if source!='1':
            parent_platformlist = ['all']
        else :                                               #点击主站可以下钻APP、轻应用、H5、PC
            parent_platformlist=['2', '3','4']

        for parent_platform in parent_platformlist:

            if parent_platform == '1':
                platformlist = ['1', '2']                      # android 、 ios

            elif parent_platform == '2':
                platformlist = ['all','3', '4','5','6','7','8','9'] # all、快应用、微信小程序、百度小程序、头条、qq、360

            elif parent_platform == '3' or parent_platform == '4' or parent_platform == '5':
                platformlist=['all']
            else:
                platformlist = ['all']

            for platform in platformlist:

                for bd_id in ['all', '1', '2']:         # 事业部id：all-all 1-出版物事业部 2-日百服 3-数字业务事业部 4-文创

                 for shop_type in ['all', '1', '2']:              # 经营方式 all-ALL 1-自营 2-招商

                     for eliminate_type in ['all', '1']:              # 剔除选项 all-all 1-剔除建工

                            for sale_type in ['sd','zf','ck']:               # 收订sd、支付zf、出库ck

                                data = {'source': source, 'parent_platform': parent_platform, 'platform': platform,
                                    'bd_id': bd_id, 'shop_type': shop_type, 'eliminate_type': eliminate_type,'sale_type':sale_type ,\
                                    'date_type':datetype,'date_str': date}
                                # data={'source': '1', 'parent_platform': '5', 'platform': 'all',
                                #     'bd_id': '4', 'shop_type': '2', 'eliminate_type': '1','sale_type':'zf' ,\
                                #     'date_type':'d','date_str': '2020-11-03'}

                                #首页
                                util.jingying_request(jingying_analysis_overview_api_url, data)

                                # util.jingying_drill_request(jingying_analysis_drill_api_url,data)

    pass

def user_analysis(datetype,date):
    user_analysis_overview_api_url = user_analysis_api_url.format('core_index')

    #共遍历4+44+4+4+4=60
    for source in ['all', '1', '2', '3', '4']:                # 下单平台 all-all 1主站 2天猫 3抖音 4拼多多
    # for source in [ '3']:

        if source != '1':
            parent_platformlist = ['all']
        else:                                                 # 点击主站可以下钻APP、小程序、H5、PC、其他
            parent_platformlist = [ '2', '3', '4']

        for parent_platform in parent_platformlist:
            if parent_platform == '1':
                platformlist = ['1', '2']                    # android 、 ios
            elif parent_platform == '2':
                platformlist = ['all','3', '4', '5', '6', '7','8','9']           # 微信小程序、百度小程序、头条、支付宝、qq、360
            else:
                platformlist = ['all']

            for platform in platformlist:

                    #core-index
                    data = {'source': source, 'parent_platform': parent_platform, 'platform': platform, 'date_str': date}
                    data['date_type'] = datetype
                    # 首页
                    util.user_request(user_analysis_overview_api_url, data)

            pass

logger = logging.getLogger('')
logger.setLevel(level=logging.INFO)


def jingying_request(final_url, data=None, jingyinglogger=None):
    datacopy = dict(data)

    datacopy['date_type'] = datacopy['date_type'][0]
    datacopy['date'] = datacopy['date_str']
    del (datacopy['date_str'])

    unexcept_value = ['--', '0', '0%', '100%', 0]

    datadict = request(final_url, datacopy)
    # print(datadict)
    if datadict and datadict['code'] == 200:

        # 取list
        datalist = datadict['data']['list']

        overviewinfo = {}
        apiinfo = {}
        for ele in datalist:
            subinfolist = ele['sub']

            for subinfo in subinfolist:
                if datacopy['shop_type'] == '2' and datacopy['sale_type'] == 'ck' and (
                        subinfo['ename'] == 'gross_profit' or subinfo['ename'] == 'gross_profit_rate'):
                    continue
                if datacopy['source'] != '1' and subinfo['ename'] == 'transRate':
                    continue

                if datacopy['parent_platform'] == '2' and subinfo['ename'] == 'transRate':
                    continue

                exceptionvaluelist = []
                if subinfo['value'] in unexcept_value:
                    exceptionvaluelist.append("value=>" + subinfo['value'])

                #  tb_hb_ignore_=subinfo['ename']

                # if tb_hb_ignore_ not in ['transRate','cancel_rate','priceByParent','priceByPerson']:

                # if subinfo['value_hb'] in unexcept_value:
                # exceptionvaluelist.append("环比=>" + subinfo['value_hb'])
                # if subinfo['value_tb'] in unexcept_value:
                # exceptionvaluelist.append("同比=>" + subinfo['value_tb'])

                if len(exceptionvaluelist) > 0:
                    name = subinfo['name']
                    apiinfo.update({name: exceptionvaluelist})
        if len(apiinfo) > 0:
            # shuchu
            # print(datacopy)
            jingyinglogger.info(datacopy['date'] + ' 异常指标:' + str(apiinfo))
            jingyinglogger.info("查询条件为:" + get_shaixuantiaojian(data))
            suburl = ''

            for key, value in datacopy.items():
                suburl += "&" + key + "=" + value
            jingyinglogger.info("api:" + final_url + suburl)
            jingyinglogger.info(' ')

def data_extract():
    start_date = '2019-01-01'
    end_date = '2019-12-31'
    date_type = ['day']  # 日、周、月、季度

    for datetype in date_type:
        datelist = util.get_day_mtd_qtd(datetype, start_date, end_date)
        # print(datelist)
        for s in datelist[31:]:
            log.setLogName(logger,'./jingying_logs/' + s + '_jingying.log')
            # 每天、每月、每季循环
            api.jingying_analysis(datetype, s, logger)
            # diff.overview_diff(datetype,date,2)
            logger.removeHandler(logger.handlers[0])
