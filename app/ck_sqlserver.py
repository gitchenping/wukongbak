from utils import util
import logging.config
from . import api
from db.dao.jingyingfenxi import *

logging.config.fileConfig("logging.conf")
userlogger=logging.getLogger('user')


def get_sqlserver_data(start,end,filter_where_for_sqlserver,cursor_sqlserver):

    if filter_where_for_sqlserver[0] %2 ==0:
        table_in='order0'
    else:
        table_in='order1'

    sqlserver_sql="select (case when orders.order_type=21 and orders.order_source like '拼%' then '拼多多' when " \
                  " orders.order_type=21 and orders.order_source like 'TM%' then 'TM订单' when " \
                  "orders.order_type=21 and orders.order_source like '抖音%' then '抖音订单' else '0' end) as _source," \
                  'items.order_quantity,' \
                  '(case when items.allot_quantity is null then 0 else items.allot_quantity  end) as _allot_quantity,items.bargin_price ' \
                  ' from '+table_in+".dbo.orders"+ " inner join "+table_in+".dbo.order_items items on orders.order_id=items.order_id " \
                " "+" where orders.order_id={} and orders.parent_id={} and items.product_id ={} and orders.cust_id={}".format(*filter_where_for_sqlserver)

    cursor_sqlserver.execute(sqlserver_sql)
    temp = cursor_sqlserver.fetchall()

    data=[]
    if len(temp)>0:
        data=list(temp[0])
        source=data[0]
        a=source.encode('latin-1').decode('gbk')              #乱码解决
        data[0]=a
        if a not in ['TM订单','拼多多','抖音订单']:
            data[0]='1'
        if data[-1] is not None:
            data[-1]=round(float(data[-1]),2)
        pass

    #出库订单商品判断

    sqlserver_sql_send="select sum(case when items.allot_quantity is null then 0 else items.allot_quantity end) as allot_quantity_ from "+table_in+".dbo.orders"+ " inner join "+table_in+".dbo.order_items items on orders.order_id=items.order_id " \
                        "where orders.send_date>='{}' and orders.send_date<='{}' and orders.order_status!=-100".format(start,end,)

    sqlserver_sql_send+=" and orders.order_id = {} and orders.parent_id = {} and items.product_id = {} and items.cust_id = {}".format(*filter_where_for_sqlserver)

    cursor_sqlserver.execute(sqlserver_sql_send)
    temp_ = cursor_sqlserver.fetchall()
    if len(data)>0:
        if len(temp_) > 0:
            data[2]=temp_[0][0]
        else:
            data[2]=0
    return data

def do_sqlserver_ck_job(start, end, conn_ck, cursor_sqlserver, ck_tables):
    order_status_dict={300:'出库',100:'支付',0:'收订'}

    ck_sql="select order_id,parent_id,product_id,cust_id," \
           "(case when source='2' then 'TM订单'  when source='4'  then '拼多多' when source='3' then '抖音订单' else '1' end ) as _source," \
           "sum(order_quantity) as sum_order_quantity,sum(allot_quantity) as sum_allot_quantity,round(sum(bargin_price),2) as sum_bargin_price from "+ck_tables+ \
            "  where last_changdate>= '{}' and last_changdate<='{}' and order_status = {} " \
             " group by order_id,parent_id,product_id,cust_id,source".format(start,end,{})

    lable=['source','order_quantity','allot_quantity','bargin_price']

    for order_status in order_status_dict.keys():
        ck_sql=ck_sql.format(order_status)

        ck_data=conn_ck.execute(ck_sql)

        for ele in ck_data:
            filter_where_for_sqlserver=ele[0:4]
            ck_data=dict(zip(lable,list(ele[4:])))

            # filter_where_for_sqlserver = [41679351863, 41679351863, 1900411307, 260273993]
            #查sqlserver
            sqlserver_raw_data=get_sqlserver_data(start,end,filter_where_for_sqlserver,cursor_sqlserver)
            sqlserver_data=dict(zip(lable,sqlserver_raw_data))
            if ck_data!=sqlserver_data:
                filter=str("order_id:"+str(filter_where_for_sqlserver[0])+" "+" parent_id:"+str(filter_where_for_sqlserver[1])
                           +" "+"product_id:"+" "+str(filter_where_for_sqlserver[2])+" "+"cust_id:"+str(filter_where_for_sqlserver[3])
                           +" "+"order_status:"+str(order_status))

                userlogger.info("查询条件:"+filter)
                userlogger.info("  sqlserver : "+str(sqlserver_data))
                userlogger.info("  clickhouse: "+str(ck_data))
                userlogger.info(' ')
                pass

    pass

def ck_sqlserver_diff():
    start = '2020-12-16 09:00:00'
    end = '2020-12-16 10:00:00'
    ck_tables='bi_mdata.kpi_order_info_all'

    #数据库连接
    conn_sqlserver, cursor_sqlserver = util.connect_sqlserver('order0')
    conn_ck = util.connect_ck_for()

    do_sqlserver_ck_job(start, end, conn_ck, cursor_sqlserver, ck_tables)

def jingying_zf_drill(data):
    datacopy=dict(data)
    jingying_zhibiao_dict = {'create_price': '金额', 'create_parent_amt': '订单量', 'create_order_amt': '包裹数',
                          "priceByParent": "单均价", "priceByPerson": "客单价"}
    saletype_dict={'sd':'收订','zf':'支付','ck':'出库'}
    name=saletype_dict[datacopy['sale_type']]
    userlogger.info(' ')
    userlogger.info("查询条件 " + str(data))

    # data={'source': '1', 'platform': '1', 'parent_platform': '1', 'bd_id': 'all', 'shop_type': 'all', 'sale_type': 'sd', 'eliminate_type': 'all', 'date_type': 'h', 'date': '2020-12-18 12'}
    for jingying_item in jingying_zhibiao_dict.keys():
        userlogger.info('经营分析-' + name + jingying_zhibiao_dict[jingying_item] + '钻取')
        data['field_str']=jingying_item
        apidata = api.jingyingfenxi(data)
        sqldata = jingyingfenxi(data)

        diff_result={}
        for item in apidata.keys():
            info,key_value_info= util.diff(apidata[item], sqldata[item])
            if len(info)>0 and len(key_value_info)>0:
                diff_result[item]=key_value_info
        if len(diff_result)>0:

             userlogger.info(str(diff_result))
             userlogger.info(
            "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-Fail-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")



def realtime_zf():
    date_str='2020-12-18'
    hour_str='12'
    ck_tables = 'bi_mdata.kpi_order_info_all'

    for source in ['1','all','3','2','4']:                 #平台来源      all-all 1主站 2天猫 3抖音 4拼多多
        if source!='1':
            parent_platformlist = ['all']
        else :                                               #点击主站可以下钻APP、轻应用、H5、PC、其他
            # parent_platformlist=[ '1', '2', '3','4']
            parent_platformlist = [ '3']

        for parent_platform in parent_platformlist:

            if parent_platform == '1':
                platformlist = ['1', '2']                      # android 、 ios

            elif parent_platform == '2':
                platformlist = ['3', '4','5','6','7','8','9'] # all、快应用、微信小程序、百度小程序、头条、qq、360

            elif parent_platform == '3' or parent_platform == '4' or parent_platform == '5' or parent_platform=='all':
                platformlist=['all']

            for platform in platformlist:

                for bd_id in ['all','1', '2', '4']:         # 事业部id：all-all 1-出版物事业部 2-日百服 3-数字业务事业部 4-文创

                    for shop_type in ['all', '1', '2']:  # 经营方式 all-ALL 1-自营 2-招商

                        for sale_type in ['sd','zf','ck']:               # 收订sd、支付zf、出库ck

                            data={'source': source, 'platform': platform,'parent_platform': parent_platform,'bd_id': bd_id, 'shop_type': shop_type,
                                  'sale_type':sale_type ,'eliminate_type': 'all','date_type':'h','date': date_str+" "+hour_str}

                            jingying_zf_drill(data)