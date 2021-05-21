#encoding=utf-8
import sys,time
import requests
import json
import sys
import os
import time
from utils.util import cmd_linux

new_uv={'新访UV': {'trend': {'05/14': 175453.0, '05/15': 123024.0, '05/16': 176725.0, '05/17': 185645.0, '05/18': 203188.0,
                              '05/19': 197267.0, '05/20': 185235.0},
            'platform': {'主站': {'value': 185235.0, '环比': -6.1, '同比上周': 2.78, '同比去年': -25.62},'APP':{'value': 189235.0,'环比': -9.1}}
        }}

uv={'活跃UV': {'trend': {'05/14': 721904.0, '05/15': 690543.0, '05/16': 702026.0, '05/17': 740582.0, '05/18': 849780.0,
                          '05/19': 814180.0, '05/20': 761370.0},
                  'platform': {'主站': {'value': 761370.0, '环比': -6.49, '同比上周': 1.87, '同比去年': -25.75},'APP':{'value': 185985.0,'环比': -6.1}}
 }}



def test(new_uv,uv):

    result={}
    result['new_uv_ratio'] = {}
    newuv=new_uv['新访UV']
    uv = uv['活跃UV']

    for item,itemvalue in newuv.items():
        if item=='trend':
            newuv_trend = itemvalue
            uv_trend = uv[item]

            trend={}
            for key in newuv_trend.keys():
                try:
                   ratio=round(newuv_trend[key] /uv_trend [key] *100,2)
                   trend[key]=ratio
                except Exception:
                    continue
            if trend != {}:
                result['new_uv_ratio'].update({'trend':trend})

        if item =="platform":
            newuv_platform = itemvalue
            uv_platform = uv[item]

            platform={}
            for key,newuvvalue in newuv_platform.items():
                platform[key]={}

                uvvalue=uv_platform[key]

                value = 0
                for vthk,vthv in newuvvalue.items():
                    try:
                       if vthk=="value":
                            value=round(newuvvalue[vthk] / uvvalue[vthk]*100,2)
                            platform[key].update({vthk:value})

                       else:
                           a=newuvvalue['value'] / (1+newuvvalue[vthk] /100)

                           b=uvvalue['value'] / (1 + uvvalue[vthk] / 100)

                           hb_tb_ratio_value= round(a/b *100,2)
                           platform[key].update({vthk:round((value/hb_tb_ratio_value -1)*100,2)})
                    except Exception:
                        continue
            if platform != {}:
                result['new_uv_ratio'].update({'platform':platform})

    return result

print(test(new_uv,uv))