from utils import date
from utils import util

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

a=diff(apidata,sqldata)
print(a)