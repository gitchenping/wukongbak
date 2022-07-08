'''
�����ʷ������ ������
'''


import sys,os,requests
import copy
import json,math,random
from db.dao.searchword_category_analysis import get_sql_data


from utils.log import set_logger
#logger
search_order_detail = set_logger(logclass='file',filename='search_order_detail')

def get_api_data(s,url,data):
    '''

    :param s:
    :param url:
    :param data: �������
    :return:
    '''
    raw_api_result = s.post(url, json = data)
    result_list = []
    api_total_num = 0
    if raw_api_result.status_code == 200:
        json_api_result = json.loads(raw_api_result.content)
        result_list = json_api_result['payload']['resultList']
        api_total_num = json_api_result['payload']['totalCount']

    return result_list,api_total_num

    pass


def do_job(starttime,endtime,s,module):

    data = {
        "aggregators": None,
        "cache": "false", "expired": 300, "flush": "false", "nativeQuery": "false",
        "groups": None,
        "orders": [],
        "filters": [],
        "params": [{"name": "start", "value": "'" + starttime + "'"},
                   {"name": "end", "value": "'" + endtime + "'"}
                   ],
        "pageSize": 10,
        "pageNo": 1
    }

    searchword_list = ['', '100�ڿ����ϼӼ�', '��ʫ�δ�������Сѧ��', '�����й�'][0:1]
    provid_id_list = ['23030','','14065',]
    platform_list = ['��׿','IOS','ȫ��']
    shop_type_list =['����','��Ӫ']

    if module =='�����ʷ������':
        groups = ["����", "ƽ̨", "һ��Ʒ��", "����Ʒ��", "����Ʒ��", "��Ӫ��ʽ", "��Ӧ�̱���/����ID", "������"]
        url="http://newwk.dangdang.com/api/v3/views/935/getdata"
        aggregators = [
                          {
                            "column": "��Ʒ�ع�PV",
                            "func": "sum"
                          },
                          {
                            "column": "������������PV",
                            "func": "sum"
                          },
                          {
                            "column": "������������UV",
                            "func": "sum"
                          },
                          {
                            "column": "��������",
                            "func": "sum"
                          },
                          {
                            "column": "����UV",
                            "func": "sum"
                          },
                          {
                            "column": "�������",
                            "func": "sum"
                          },
                          {
                            "column": "���UV",
                            "func": "sum"
                          },
                          {
                            "column": "����UV�����",
                            "func": "sum"
                          },
                          {
                            "column": "�ն��û���",
                            "func": "sum"
                          },
                          {
                            "column": "�ն��û�ת����",
                            "func": "sum"
                          },
                          {
                            "column": "�ն�����",
                            "func": "sum"
                          },
                          {
                            "column": "�ն����",
                            "func": "sum"
                          },
                          {
                            "column": "��Ʒ�ն�����",
                            "func": "sum"
                          },
                          {
                            "column": "�����޽������",
                            "func": "sum"
                          },
                          {
                            "column": "�޽����������ռ��",
                            "func": "sum"
                          },
                          {
                            "column": "�ع����λ����λ��",
                            "func": "sum"
                          },
                          {
                            "column": "������λ����λ��",
                            "func": "sum"
                          },
                          {
                            "column": "ƽ����Ʒ�ع����λ��",
                            "func": "sum"
                          },
                          {
                            "column": "ƽ����Ʒ������λ��",
                            "func": "sum"
                          },
                          {
                            "column": "������",
                            "func": "sum"
                          },
                          {
                            "column": "RPM",
                            "func": "sum"
                          },
                          {
                            "column": "����UV��ֵ",
                            "func": "sum"
                          },
                          {
                            "column": "֧�����",
                            "func": "sum"
                          },
                          {
                            "column": "ʵ�����",
                            "func": "sum"
                          }

                        ]
    elif module == 'uv_pv����ͼ':
        groups = ["����"]
        url = "http://newwk.dangdang.com/api/v3/views/935/getdata"
        aggregators = [{'column': "����PV", 'func': "sum"}, {'column': "����UV", 'func': "sum"}]
    elif module =='top������':
        groups = ["������"]
        url = "http://newwk.dangdang.com/api/v3/views/936/getdata"
        aggregators = [{'column': "��������", 'func': "sum"}]

    for searchword in searchword_list:
        for platform in platform_list:
            for shop_type in shop_type_list:
                for provid_id in provid_id_list:

                    temp_data = copy.deepcopy(data)
                    if searchword != '':
                        temp_data['filters'] = [{'name': "search_word", 'type': "filter", 'value': "'{}'".format(searchword),
                                            'sqlType': "STRING", 'operator': "="}]
                    if provid_id !='':
                        temp_data['params'].append({'name': "provid_id", 'value': "'{}'".format(provid_id)})

                    temp_data['params'].append({'name': "platform_name", 'value': "'{}'".format(platform)})
                    temp_data['params'].append({'name': "product_is_merchant_name", 'value': "'{}'".format(shop_type)})
                    temp_data['params'].append({'name': "path1_name", 'value': "'ͼ��'"})
                    temp_data['groups'] = groups
                    temp_data['aggregators'] = aggregators

                    #sql data
                    df,params_dict = get_sql_data(temp_data,module)

                    msg = module + ' ɸѡ������' + str(params_dict)
                    #��ѡ����ҳ����в���
                    dev_total_page = 1
                    pageno = 1

                    while pageno <= dev_total_page:
                        api_result_list,api_result_total_num = get_api_data(s,url,temp_data)

                        length_api_result = len(api_result_list)
                        if length_api_result == 0 and not df.empty:
                            search_order_detail.info(msg+" -Fail")
                            search_order_detail.info("dev empty")
                        if module in ['�����ʷ������']:
                            if pageno == 1:
                                dev_total_page = math.ceil(api_result_total_num / 20)
                            pageno += dev_total_page // 10 + 1  # ��ҳ
                            temp_data['pageNo'] = pageno

                            min_test_num = 2
                        else:
                            pageno = 2
                            min_test_num = 50

                        choice = random.sample([i for i in range(0, length_api_result)], min(min_test_num, length_api_result))

                        for i in choice:
                            api_data  = {key:value if not isinstance(value,float) else round(value,2) for key,value in api_result_list[i].items() }

                            #
                            bool_series = [df[ele] == api_data[ele] for ele in groups]
                            a = True
                            for bs in bool_series:
                                a &= bs

                            sqlresult_match = df[a]
                            if not sqlresult_match.empty:
                                sqlresult_match = sqlresult_match.where(sqlresult_match.notnull(), '')
                                sql_data = sqlresult_match.iloc[0].to_dict()
                            else:
                                sql_data = {}

                            diffvalue = diff(sql_data,api_data)

                            if diffvalue != {}:

                                if os.name == 'posix':
                                    search_order_detail.info(msg+" -Fail")
                                    search_order_detail.info(diffvalue)
                                    search_order_detail.info('')
                                else:

                                    print(diffvalue)
                            else:
                                search_order_detail.info(msg+ " -Pass")



if __name__ == '__main__':
    starttime = '2022-07-01'
    endtime = '2022-07-01'

    token = ""

    s = requests.Session()
    s.headers["Content-Type"] = "application/json"
    s.headers['Authorization'] = "Bearer " + token

    for module in ['�����ʷ������','uv_pv����ͼ','top������'][1:2]:
        do_job(starttime, endtime, s,module)