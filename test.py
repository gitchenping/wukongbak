from utils import date
from utils import util
from utils.log import log,myapp
from utils import map

num=0
for source in ['all', '1', '2', '3', '4']:  # 平台来源      all-all 1主站 2天猫 3抖音 4拼多多
    if source != '1':
        parent_platformlist = ['all']
    else:  # 点击主站可以下钻APP、轻应用、H5、PC
        parent_platformlist = ['2', '3', '4']

    for parent_platform in parent_platformlist:

        if parent_platform == '1':
            platformlist = ['1', '2']  # android 、 ios

        elif parent_platform == '2':
            platformlist = ['5', 'all', '3', '4', '6', '7', '8', '9']  # all、3-快应用、4-微信小程序、5-百度小程序、6-头条、7-qq、8-360

        else:
            platformlist = ['all']

        for platform in platformlist:

            for bd_id in ['1', '2', '4', 'all']:  # 事业部id：all-all 1-出版物事业部 2-日百服 3-数字业务事业部 4-文创

                for shop_type in ['all', '1', '2']:  # 经营方式 all-ALL 1-自营 2-招商

                    for eliminate_type in ['all', '1']:  # 剔除选项 all-all 1-剔除建工

                        for sale_type in ['ck',]:  # 收订sd、支付zf、出库ck
                            num+=1


print(num)
mydate='2021-01-06'
data1={'a':123,'b':134.6,'c':100}
data2={'a':124,'b':135.7,'c':100}

data3={'a':123,'b':{'bb1':236,'bb2':90},'c':{'cc1':{'cc11':3458,'cc22':200}},'d':{'dd1':236,'dd2':90}}
data4={'a':123,'b':{'bb1':236,'bb2':90},'c':{'cc1':{'cc11':3458,'cc22':200}},'d':{'dd1':236,'dd2':90}}
apidata={'支付金额': {'trend':
                      {'W48': 771732.08, 'W49': 627223.67, 'W50': 969113.61,
                       'W51': 714615.62, 'W52': 799648.44, 'W53': 716077.3, 'W1': 670073.85}
}}
sqldata={'支付金额': {'trend': {'W48': 771742.08, 'W49': 627223.67, 'W50': 969113.61,
                            'W51': 714615.62, 'W52': 799649.44, 'W53': 716077.3, 'W1': 670073.85}}}

def diff(data1,data2,absvalue=0.5):
    # 字典比较
    temp_data1 = dict(data1)
    temp_data2 = dict(data2)
    # diff_key_value_list=[]
    diff_key_value={}
    for key in temp_data1.keys():
        try:
            data1_value=temp_data1[key]
            data2_value=temp_data2[key]
            if isinstance(data1_value,dict):
                # diff_key_value_list.extend(diff(temp_data1[key],temp_data2[key]))
                diff_key_value[key]=diff(data1_value,data2_value)
                if diff_key_value[key]=={}:
                    diff_key_value.pop(key)
            else:
                if abs(data1_value-data2_value) > absvalue:
                    # diff_key_value_list.append({key: (temp_data1[key], temp_data2[key])})
                    diff_key_value[key]=(data1_value, data2_value)
        except Exception as e:
            # print(e.__repr__())
            key_error_string=key+' 键值对不存在'
            diff_key_value[key_error_string]=e.__repr__()
    return diff_key_value

myapp.info({'a':1234})