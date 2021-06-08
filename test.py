#encoding=utf-8
import sys,time
import requests
import json
import sys
import os
import time
from utils.tb_hb import get_tb_hb_key_dict
from utils.util import is_number
from db.dao._sql import data_merge
from resources.map import bd_id_cat,parent_platform_cat

data = {'source': '4', 'platform': 'all', 'parent_platform': 'all', 'bd_id': 'all', 'shop_type': '1',
        'eliminate_type': 'all', 'date_type': 'd', 'date': '2021-05-12', 'field_str': 'uv'}

date_hour = data['date'].split(' ')       #对于时的情况，传值形式为2021-05-21 13

date=date_hour[0]
datetype = data['date_type']

ename=data['field_str']
sqldata={}

rawdata=[
                     [360,'2021-05-24',],
                      # [300,'2021-05-17'],
                     [198,'2021-05-17'],
                    # ['NaN','2020-05-12']
    ]

rawdata2=[
          [1,12,100,'2021-05-12'],
            [1,20,130,'2021-05-12'],
            [1,12,170,'2021-05-11'],
           [1,20,100,'2021-05-11'],
          [2,3,120,'2021-05-12'],
         [2,4,80,'2021-05-11'],
         [2,4,None,'2021-05-11'],
           [2,5,110,'2021-05-05']
          ]

namedict={
    '1':'安卓',
    '2':'IOS'
}

def data_merge(ck_data):
    '''
    根据时间日期、首位分类进行数据合并
    :param ck_data: 细分的数据，加和 合并，如[['1','3',100,'2021-06-06'],['1','20',120,'2021-06-06'],
            ['1','4',100,'2021-06-06'],['1','3',100,'2021-06-05'],['1','20',100,'2021-06-05'],
            ['2','5',110,'2021-06-06'],['2','6',80,'2021-06-06'],['2','5',200,'2021-06-05']]
    :return:[['1',320,'2021-06-06'],['1',200,'2021-06-05'],['2',190,'2021-06-06'],['2',200,'2021-06-05']]
    '''

    # 数据预处理
    ckdata = [[ele[0], float(ele[-2]), ele[-1]] for ele in ck_data if is_number(ele[-2]) and ele[1] != '0']
    # 依据时间排序、分类排序
    ckdata = sorted(ckdata, key=lambda s: s[2], reverse=True)
    ckdata = sorted(ckdata, key=lambda s: s[0])

    length=len(ckdata)
    new_ckdata=[]

    cmp=ckdata[0]

    for ele in ckdata[1:]:

        if ele[0] == cmp[0] and ele[-1] == cmp[-1]:
            cmp[1] +=ele[1]

        else:
            new_ckdata.append(cmp)




    return new_ckdata

result=data_merge(rawdata2)
a=12


