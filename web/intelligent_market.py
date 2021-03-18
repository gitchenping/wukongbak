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

queryjson=data['queryJson']
query_data=json.loads(queryjson)

query_data=d

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
                temp["level"]= "condition"
                temp["relation"]="key1"

                alist.append(temp)
            else:
                templist=parse_jsonlabel_to_list(value)
                for ele in templist:
                    temp={key:ele}
                    alist.append(temp)
        return alist

a=parse_jsonlabel_to_list(query_data)
print(a)
print(operlist)