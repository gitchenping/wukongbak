from utils import util,log
from kafka import KafkaProducer
import json
import requests,pymysql
from utils.decorate import complog
from utils.util import simplediff
#日志设置
oper = log.set_logger('operation.txt')
#mysql conn
mysql_cursor = util.connect_mysql(host="10.255.254.225",port=3308, user="writeuser", password="ddbackend", database="ioc_adm")
ck_client=util.client_ck(host='10.7.30.148', user='membersbi',password='dangdangbi', database='ioc')
#ck conn


#kafka集群地址
kafka_hosts=['10.255.242.91:9092','10.255.242.92:9092','10.255.242.93:9092','10.255.242.94:9092','10.255.242.95:9092']

TableName='smart_cust_detail'
CkTableName='ioc.user_tag_base_info_all'
mysql_table='ioc_adm.smart_cust_detail'

def get_mysql_phone(crowdid,num):
    '''

    :param crwodid: 人群id,根据这个找表，找custid\phone
    :param num: 人群数量
    :return: {cust_id:phone}
    '''
    phone_column="phone,phone_shippingAdr_default,phone_shippingAdr_new"
    mysql_data={}
    access_num= int(num)

    mysql_data={}
    if access_num >0:
        mod_crowdid=int(crowdid) % 100

        tablename_suffix=mod_crowdid <10 and '0'+str(mod_crowdid) or str(mod_crowdid)
        tablename=TableName+"_"+tablename_suffix

        sql_restul_num_limit = ' limit 600,610'
        where=" where crowd_id="+str(crowdid)

        sql="select cust_id,"+phone_column+" from "+tablename+ where+sql_restul_num_limit

        row_data=[]
        try:
            mysql_cursor.execute(sql)
            row_data =mysql_cursor.fetchall()
        except pymysql.err.OperationalError:
            print("超时")
        except Exception as e:
            print(e.__repr__())

        if row_data != ():
            # mysql_data = dict(row_data)
            for ele in row_data:
                mysql_data[ele[0]]={
                    'phone':ele[1],
                    'phone_default':ele[2],
                    'phone_new':ele[3]
                }
    return mysql_data

def get_ck_phone(custiddata):
    '''
    :param custiddata: from mysql data
    :return:
    '''
    phone_column = "mobile_phone,default_mobile,last_mobile"
    ck_data={}
    rawdata=[]
    where=' where cust_id in ('
    if custiddata!={}.keys():
        for ele in custiddata:
            where+=str(ele)+","

        sql=" select cust_id,"+phone_column+" from "+CkTableName+ where.strip(',')+")"
        try:
            rawdata=ck_client.execute(sql)
        except Exception as e:
            print(e.__repr__())

        if rawdata!=[]:
            # ckdata=dict(rawdata)
            for ele in rawdata:
                ck_data[ele[0]]={
                    'phone':ele[1],
                    'phone_default':ele[2],
                    'phone_new':ele[3]
                }
    return ck_data

@complog(oper)
def phone_check(crowdid, num):
    mysql_data=get_mysql_phone(crowdid, num)
    # print(mysql_data)
    ck_data = get_ck_phone(mysql_data.keys())

    diff_value = simplediff(mysql_data, ck_data)
    return diff_value

def asyncjob(logger,result):
    crowdid=result[0][0]
    #num
    num=result[1]['result']['amount']

    diff=phone_check(crowdid,num)
    logger.info("crowdid:" + str(crowdid))
    if diff!={}:
        #存在phone不一致的key
        logger.info(diff)
    else:
        logger.info('--total num:'+str(num)+' --pass--')
    logger.info('')


def get(fun):
    url='http://10.5.60.168:8880/demo/showCrowdVo'

    req=requests.get(url)
    jsondata=json.loads(req.text)

    #
    data=fun(jsondata)

    #写库
    url='http://10.5.60.168:8880/demo/addCrowdVo'

    msg=requests.post(url, json=data).text
    msg_content=json.loads(msg)

    crowdid=-1
    if msg_content['errorMsg']=='成功':
        crowdid=jsondata['id']

    return crowdid