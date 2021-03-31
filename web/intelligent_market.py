a={"filterMap":{
                    "B":{"filterMap":{
                                "key1":{"filterMap":{"pay-date":{"oper":"between","value":"2021-02-03 00:00:00","value2":"2021-03-04 23:59:59"},
                                                     "payProductBrand":{"oper":"in","value":"当当"}},"group":{"oper":">=","type":"0","value":"1"},
                                        "level":"condition","relation":"pay-date#&#payProductBrand"}},
                        "level":"collection","relation":"key1"
                    }
                },
    "level":"collection","relation":"B"}

b={'filterMap': {'A': {'filterMap': {'key1': {'filterMap': {'userGender': {'oper': '=', 'value': 'male'}}, 'level': 'condition', 'relation': 'userGender'},
                                     'key2': {'filterMap': {'userGrade': {'oper': 'in', 'value': '1年级'}}, 'level': 'condition', 'relation': 'userGrade'}},
                       'level': 'collection', 'relation': 'key1#|#key2'}},
   'level': 'collection', 'relation': 'A'}

c={'filterMap': {'A': {'filterMap': {'key1': {'filterMap': {'userGender': {'oper': '=', 'value': 'male'}}, 'level': 'condition', 'relation': 'userGender'},
                                     'key2': {'filterMap': {'userGrade': {'oper': 'in', 'value': '1年级'}}, 'level': 'condition', 'relation': 'userGrade'}},
                       'level': 'collection', 'relation': 'key1#&#key2'},
                 'B': {'filterMap': {'key1': {'filterMap': {'pay-date': {'oper': 'between', 'value': '2021-01-31 00:00:00', 'value2': '2021-03-01 23:59:59'},
                                                            'payProductBrand': {'oper': 'in', 'value': '当当'}}, 'group': {'oper': '>=', 'type': '0', 'value': '1'},
                        'level': 'condition', 'relation': 'pay-date#&#payProductBrand'}}, 'level': 'collection', 'relation': 'key1'}},
   'level': 'collection', 'relation': 'A#&#B'}

d={'filterMap': {'A': {'filterMap': {'key1': {'filterMap': {'userGender': {'oper': '=', 'value': 'male'}}, 'level': 'condition', 'relation': 'userGender'},
                                     'key2': {'filterMap': {'userPlatformMemRank': {'oper': 'in', 'value': '2'}}, 'level': 'condition', 'relation': 'userPlatformMemRank'},
                                     'key3': {'filterMap': {'userGrade': {'oper': 'in', 'value': '1年级'}}, 'level': 'condition', 'relation': 'userGrade'},
                                     'key4': {'filterMap': {'register-date': {'oper': 'between', 'value': '2020-08-30 00:00:00', 'value2': '2021-01-24 23:59:59'},
                                                            'registerAppType': {'oper': 'in', 'value': '2002'}}, 'level': 'condition', 'relation': 'register-date#&#registerAppType'}},
                       'level': 'collection', 'relation': 'key1#&#key2#&#key3#&#key4'}}, 'level': 'collection', 'relation': 'A'}

# searchword_api = "http://10.5.60.168:8880/demo/showCrowdVo"
# cal_api ="http://ioc.dangdang.cn/smart/crowd/calc?type=1"
# save_api="http://ioc.dangdang.cn/smart/crowd/save?type=0"
post_api = "http://10.5.60.46:8880/demo/sqlDesc"
# add_url = 'http://10.5.60.46:8880/demo/addCrowdVo'
# searchword_api = 'http://10.5.60.46:80/smart/crowd/save?type=0'

def oper(relationstr):
    default_oper=['&']
    if relationstr=='key1':
        return []
    oper_list=[ele for ele in relationstr.split('#') if ele in ['&', '|']]
    if oper_list==[]:
        oper_list=default_oper
    return oper_list

operlist=[]
def parse_jsonlabel_to_list(query_data):
    '''将标签字典展开'''
    alist=[]
    if 'filterMap' in query_data:
        #
        operlist.extend(oper(query_data['relation']))
        #
        for key,value in query_data['filterMap'].items():
            temp={}
            templist=[]
            if key.startswith('key') :
                key = 'key1'
                temp['filterMap'] = {key: value}
                temp["level"]= "collection"
                temp["relation"]="key1"

                alist.append(temp)
            else:
                templist=parse_jsonlabel_to_list(value)
                for ele in templist:
                    temp={key:ele}
                    alist.append(temp)
        return alist
import requests
import json
from kafka import KafkaProducer,KafkaConsumer

from resources.book_set import child_book,xiaoshuo,preg_child_book,cloth_shoe_book,publish_book,food_book,stock_id_book,search_subc_stock_id


#debug
def test_label(jsondata):
    jsondata['sequenceB'] = []
    jsondata['name'] = 'test_debug'
    jsondata['sequenceA'] = ["gender"]
    jsondata['labels']['label'][0]['labelsList'][0]['checkData']['selectedVal']=[2]

    return jsondata
#浏览
def skim(jsondata):
    # 插入一个字段
    location=jsondata['labels']['label'][1]['labelsList'][0]['checkData']
    jsondata['name'] = 'test_skim'

    jsondata['sequenceB'] = ["skim"]
    jsondata['sequenceA'] = []

    location['dataNumber']['dynamic'] = {"firstDay": 2, "firstNumber": "45", "secondDay": 1, "secondNumber": None}
    location['dataNumber']['timeType']=3
    location['labelNumber']={"compareValueA": "1", "number": 1, "compare": 1}
    location['selectedVal']=[{"firstVal": 1, "scdVal": [1],"fthVal":"","thirdVal":xiaoshuo}]

    return jsondata

#年纪-搜索-推送点击+支付+出库
def grade_search_push_pay_stock(jsondata):
    grade_location=jsondata['labels']['label'][0]['labelsList'][2]['checkData']
    search_location=jsondata['labels']['label'][1]['labelsList'][1]
    push_location=jsondata['labels']['label'][1]['labelsList'][4]
    pay_location=jsondata['labels']['label'][2]['labelsList'][1]
    stock_location=jsondata['labels']['label'][2]['labelsList'][2]

    jsondata['name'] = 'test_grade_search_push_pay_stock'
    jsondata['sequenceB'] = ["search","push","pay","stock"]
    jsondata['sequenceA'] = ["grade"]

    jsondata['labels']['association'] = "OR"

    grade_location['selectedVal']=[1,2,3]

    search_location['association']="AND"
    search_location['checkData']['dataNumber']['timeType'] = 1
    search_location['checkData']['dataNumber']['fixed']={"dateWidget":["2020-11-05", "2021-02-03"]}
    search_location['checkData']['labelNumber']= {"compareValueA": "1", "number": 1, "compare": 1}
    search_location['checkData']['selectedVal']=[{"firstVal": 2, "scdVal": [2], "thirdVal": ["玩具"]}]

    push_location['assoc']="OR"
    push_location['checkData']['dataNumber']['timeType'] = 1
    push_location['checkData']['dataNumber']['fixed'] = {"dateWidget": ["2020-11-05", "2021-02-03"]}
    push_location['checkData']['labelNumber'] = {"compareValueA": "1", "number": 1,"compare": 2}

    pay_location['assoc'] = "OR"
    pay_location['checkData']['dataNumber']['timeType'] = 1
    pay_location['checkData']['dataNumber']['fixed'] = {"dateWidget": ["2020-09-01", "2021-01-31"]}
    pay_location['checkData']['labelNumber'] = {"compareValueA": "50", "number": 5,"compare": 1}
    pay_location['checkData']['selectedVal']=[{"firstVal": 5, "scdVal": [1],"fthVal":"","thirdVal":child_book}]

    stock_location['assoc'] = "OR"
    stock_location['checkData']['dataNumber']['timeType'] = 1
    stock_location['checkData']['dataNumber']['fixed'] = {"dateWidget": ["2019-01-01", "2021-01-31"]}
    stock_location['checkData']['labelNumber'] = {"compareValueA": "50", "number": 5, "compare": 1}
    stock_location['checkData']['selectedVal'] = [{"firstVal": 5, "scdVal": [1], "fthVal": "", "thirdVal": preg_child_book}]

    return jsondata

def stock(jsondata):
    jsondata['name'] = 'test_stock'
    jsondata['sequenceB'] = ["stock"]
    jsondata['sequenceA'] = []

    stock_location = jsondata['labels']['label'][2]['labelsList'][2]

    stock_location['checkData']['dataNumber']['timeType'] = 1
    stock_location['checkData']['dataNumber']['fixed'] = {"dateWidget": ["2019-04-01", "2021-01-29"]}
    stock_location['checkData']['labelNumber'] = {"compareValueA": "1", "number": 1, "compare": 1}
    stock_location['checkData']['selectedVal'] = [{"firstVal": 5, "scdVal": [1], "thirdVal": cloth_shoe_book, "fthVal": ""}]

    return jsondata

def stock_with_productid(jsondata):
    jsondata['name'] = 'test_stock_with_productid'
    jsondata['sequenceB'] = ["stock"]
    jsondata['sequenceA'] = []

    stock_location = jsondata['labels']['label'][2]['labelsList'][2]

    stock_location['checkData']['dataNumber']['timeType'] = 1
    stock_location['checkData']['dataNumber']['fixed'] = {"dateWidget": ["2019-01-01", "2021-01-11"]}
    stock_location['checkData']['labelNumber'] = {"compareValueA": "1", "number": 1, "compare": 1}
    stock_location['checkData']['selectedVal'] = [
        {"firstVal": 5, "scdVal": [3], "thirdVal": stock_id_book, "fthVal": ""}]

    return jsondata


def search_subscribe_stock(jsondata):
    jsondata['name'] = 'test_search_subscribe_stock'
    jsondata['sequenceB'] = ["search","subscribe","stock"]
    jsondata['sequenceA'] = []

    search_location = jsondata['labels']['label'][1]['labelsList'][1]
    subscribe_location=jsondata['labels']['label'][2]['labelsList'][0]
    stock_location=jsondata['labels']['label'][2]['labelsList'][2]

    search_location['checkData']['dataNumber']['timeType'] = 1
    search_location['checkData']['dataNumber']['fixed'] = {"dateWidget": ["2017-01-01", "2021-02-08"]}
    search_location['checkData']['labelNumber'] = {"compareValueA": "1", "number": 1, "compare": 1}
    search_location['checkData']['selectedVal'] = [{"firstVal": 2, "scdVal": [2], "thirdVal": ["石黑一雄"]}]

    subscribe_location['assoc'] = "OR"
    subscribe_location['checkData']['dataNumber']['timeType'] = 1
    subscribe_location['checkData']['dataNumber']['fixed'] = {"dateWidget": ["2020-11-01", "2021-03-02"]}
    subscribe_location['checkData']['labelNumber'] = {"compareValueA": "1", "number": 1, "compare": 1}
    subscribe_location['checkData']['selectedVal'] = [{"firstVal": 5,"scdVal": [1], "thirdVal": publish_book, "fthVal": ""}]

    stock_location['assoc'] = "OR"
    stock_location['checkData']['dataNumber']['timeType'] = 1
    stock_location['checkData']['dataNumber']['fixed'] = {"dateWidget": ["2017-01-01", "2021-02-08"]}
    stock_location['checkData']['labelNumber'] = {"compareValueA": "1", "number": 1, "compare": 1}
    stock_location['checkData']['selectedVal'] = [{"firstVal": 5, "scdVal": [3], "thirdVal": search_subc_stock_id, "fthVal": ""}]

    return jsondata

def shopcart_pay(jsondata):
    shopcart_location = jsondata['labels']['label'][1]['labelsList'][2]['checkData']
    pay_location = jsondata['labels']['label'][2]['labelsList'][1]

    jsondata['name'] = 'test_shopcart_pay'
    jsondata['sequenceB'] = ["shoppingcart","pay"]
    jsondata['sequenceA'] = []

    shopcart_location['dataNumber']['timeType']=3
    shopcart_location['dataNumber']['dynamic'] = {"firstDay": 2, "firstNumber": "7", "secondDay": 2, "secondNumber": 2}
    shopcart_location['labelNumber'] = {"compareValueA": "1", "number": 1, "compare": 1}
    shopcart_location['selectedVal'] = []

    pay_location['assoc'] = "NOT"
    pay_location['checkData']['dataNumber']['timeType'] = 3
    pay_location['checkData']['dataNumber']['dynamic'] = {"firstDay": 2, "firstNumber": "7", "secondDay": 1, "secondNumber": None}
    pay_location['checkData']['labelNumber'] = {"compareValueA": "1", "number": 1, "compare": 1}
    pay_location['checkData']['selectedVal'] =[]

    return jsondata

def pay_stock(jsondata):
    jsondata['name'] = 'test_pay_stock'
    jsondata['sequenceB'] = ["stock", "pay"]
    jsondata['sequenceA'] = []

    pay_location = jsondata['labels']['label'][2]['labelsList'][1]
    stock_location = jsondata['labels']['label'][2]['labelsList'][2]

    pay_location['checkData']['dataNumber']['timeType'] = 1
    pay_location['checkData']['dataNumber']['fixed'] = {"dateWidget": ["2020-01-01", "2021-01-01"]}
    pay_location['checkData']['labelNumber'] = {"compareValueA": "3", "number": 1, "compare": 1}
    pay_location['checkData']['selectedVal'] = [{"firstVal": 5, "scdVal": [1], "fthVal": "", "thirdVal": child_book+preg_child_book}]

    stock_location['checkData']['dataNumber']['timeType'] = 1
    stock_location['checkData']['dataNumber']['fixed'] = {"dateWidget": ["2020-01-01", "2021-01-01"]}
    stock_location['checkData']['labelNumber'] = {"compareValueA": "99", "number": 5, "compare": 1}
    stock_location['checkData']['selectedVal'] = [{"firstVal": 5, "scdVal": [1], "fthVal": "", "thirdVal": child_book+preg_child_book}]

    return jsondata

def subscribe_stock(jsondata):
    jsondata['name'] = 'test_subscribe_stock'
    jsondata['sequenceB'] = ["stock", "subscribe"]
    jsondata['sequenceA'] = []

    subscribe_location=jsondata['labels']['label'][2]['labelsList'][0]['checkData']
    stock_location = jsondata['labels']['label'][2]['labelsList'][2]['checkData']

    subscribe_location['dataNumber']['timeType'] = 1
    subscribe_location['dataNumber']['fixed'] = {"dateWidget": ["2020-09-25", "2020-12-24"]}
    subscribe_location['labelNumber'] = {"compareValueA": "2", "number": 1, "compare": 1}
    subscribe_location['selectedVal'] = [{"firstVal": 5, "scdVal": [1], "thirdVal": food_book, "fthVal": ""}]

    stock_location['dataNumber']['timeType'] = 1
    stock_location['dataNumber']['fixed'] = {"dateWidget": ["2020-09-25", "2020-12-24"]}
    stock_location['labelNumber'] = {"compareValueA": "2", "number": 1, "compare": 1}
    stock_location['selectedVal'] = [{"firstVal": 5, "scdVal": [1], "fthVal": "", "thirdVal": cloth_shoe_book + preg_child_book+cloth_shoe_book}]

    return jsondata

def shopcart_collection_pay_stock(jsondata):
    shopcart_location = jsondata['labels']['label'][1]['labelsList'][2]['checkData']
    pay_location = jsondata['labels']['label'][2]['labelsList'][1]
    collection_location=jsondata['labels']['label'][1]['labelsList'][3]
    stock_location = jsondata['labels']['label'][2]['labelsList'][2]

    jsondata['name'] = 'test_shopcart_collection_pay_stock'
    jsondata['sequenceB'] = ["shoppingcart","stock","pay","collection"]
    jsondata['sequenceA'] = []

    shopcart_location['dataNumber']['timeType'] = 1
    shopcart_location['dataNumber']['fixed'] = {"dateWidget": ["2020-09-01", "2020-10-12"]}
    shopcart_location['labelNumber'] = {"compareValueA": "2", "number": 1, "compare": 1}
    shopcart_location['selectedVal'] = [{"firstVal": 2, "scdVal": [1], "fthVal": "", "thirdVal": preg_child_book}]

    collection_location['assoc']="OR"
    collection_location['checkData']['dataNumber']['fixed'] = {"dateWidget": ["2020-09-01", "2020-10-12"]}
    collection_location['checkData']['labelNumber'] = {"compareValueA": "2", "number": 1, "compare": 1}
    collection_location['checkData']['selectedVal'] = [{"firstVal": 1, "scdVal": [1], "fthVal": "", "thirdVal": preg_child_book}]

    pay_location['assoc'] = "OR"
    pay_location['checkData']['dataNumber']['timeType'] = 1
    pay_location['checkData']['dataNumber']['fixed'] = {"dateWidget": ["2020-01-01", "2020-10-12"]}
    pay_location['checkData']['labelNumber'] = {"compareValueA": "50", "number": 5, "compare": 1}
    pay_location['checkData']['selectedVal'] = [{"firstVal": 5, "scdVal": [1], "fthVal": "", "thirdVal": preg_child_book}]

    stock_location['assoc'] = "OR"
    stock_location['checkData']['dataNumber']['timeType'] = 1
    stock_location['checkData']['dataNumber']['fixed'] = {"dateWidget": ["2020-07-01", "2020-10-13"]}
    stock_location['checkData']['labelNumber'] = {"compareValueA": "2", "number": 2, "compare": 1}
    stock_location['checkData']['selectedVal'] = [{"firstVal": 7, "scdVal": [6], "thirdVal": [1, 2], "fthVal": ""},
                                                  {"firstVal": 5, "scdVal": [1], "fthVal": "", "thirdVal": preg_child_book}]
    return jsondata

def couponbath(jsondata):
    jsondata['name'] = 'test_couponbath'
    jsondata['sequenceB'] = ["CouponBatch"]
    jsondata['sequenceA'] = []

    stock_location = jsondata['labels']['label'][4]['labelsList'][0]['checkData']
    stock_location['labelNumber']={"compareValueA": 1, "number": 0, "compare": 0, "compareValueB": "193432"}

    return jsondata



def get(fun):
    url='http://10.5.60.168:8880/demo/showCrowdVo'

    req=requests.get(url)
    jsondata=json.loads(req.text)

    #
    data=fun(jsondata)

    res_data=post(data)
    retdata = {
        'queryJson': res_data,
        'crowdId': jsondata['id'],
        'channelCode': 0,
        'applicantId': 'tester'}
    return retdata
def count_crowd(jsondata):
    url = "http://10.5.24.30:8881/crowd/amount"

    req = requests.post(url=url, data=jsondata, headers={"Content-Type": 'application/x-www-form-urlencoded'})

    if req.status_code == 200:
        res_data = req.content.decode('utf-8')

        b = json.loads(res_data)
        return b

def send_kafka(data):
    send_data = {
        'queryJson': json.dumps(data),
        'crowdId':'f263ccdae07623fe15316a3cb5f55f1b',
        'channelCode': 0,
        'applicantId': 'tester',
        "queryId":16413,
        "departmentId":0}
    kafka_broker_list=['10.255.242.91:9092','10.255.242.92:9092','10.255.242.93:9092',
                       '10.255.242.94:9092','10.255.242.95:9092']
    producer = KafkaProducer(bootstrap_servers=kafka_broker_list)
    msg=json.dumps(send_data).encode('utf-8')    #需要发送为字节码
    producer.send('ioc-smart-cal-detail', msg)
    producer.close()
def consumer_kafka():
    kafka_topic='ioc-smart-cal-detail'
    group_name='ioc-smart-cal-detail_group'
    kafka_broker_list = ['10.255.242.91:9092', '10.255.242.92:9092', '10.255.242.93:9092',
                         '10.255.242.94:9092', '10.255.242.95:9092']
    consumer = KafkaConsumer(kafka_topic,group_id=group_name,bootstrap_servers=kafka_broker_list)

    for message in consumer:
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))