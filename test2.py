import sys


def recursionBS(L, number, start, end):
    '''

    :param L:数组列表
    :param number:待查找数字
    :param start:查找起始位置
    :param end:查找结束位置
    :return:
    '''
    if  start > end:
        return -1
    mid = (start + end) // 2
    if L[mid] == number:
        return mid
    elif L[mid] > number:
        left = recursionBS(L, number, start, mid - 1)
        return left
    else:
        return recursionBS(L, number, mid + 1, end)


arr =[2,6,9,12]
print(recursionBS(arr,2,0,len(arr)-1))