#encoding=utf-8
import sys,time
import requests
import json

from utils import util,log

from utils.util import simplediff
#日志设置
from multiprocessing import Pool,Manager
import time,os
import random


loggername = "logger_lock"
oper = log.set_logger(loggername)






def test(i, lock,length,test_result):
    custid_data = {}

    j = i * 3
    if j < length:

        ele = test_result[j]
        custid = ele[0]
        custid_data[custid] = {ele[1]: ele[2]}

        loop_down = 1
        # 向下找相同的custid
        while loop_down <= 2 and j + loop_down <= length - 1:
            nextele = test_result[j + loop_down]

            if nextele[0] != custid:
                break
            custid_data[custid].update({nextele[1]: nextele[2]})
            loop_down += 1

        # 向上找3 - loop_down 个
        loop_up = 1
        while loop_up <= 3 - loop_down and j - loop_up >= 0:
            forwardele = test_result[j - loop_up]

            if forwardele[0] != custid:
                break

            custid_data[custid].update({forwardele[1]: int(forwardele[2])})
            loop_up += 1
        print(custid_data)
        oper.info('now check:' + str(custid))

    # lock.acquire()
    #
    # diff_key_value = util.diff(custid_data, custid_data)
    # oper.info('now check:' + str(custid))
    # if diff_key_value != {}:
    #     oper.info(diff_key_value)
    #     oper.info(' ')
    #
    # lock.release()

    # dev_data = {}
    # dev_data[custid] = {}
    # for ele in dev_result:
    #     dev_data[custid].update({ele[0]: int(ele[1])})
    #
    # # diff
    # diff_key_value = diff(custid_data, dev_data)
def func(msg,lock,date):
    print(date)
    # print("msg:", str(msg))
    #
    # j = msg * 3
    # ele = test_result[j]
    # custid = ele[0]
    # # custid_data[custid] = {ele[1]: ele[2]}
    #
    # oper.info('now check:' + str(custid))
    #
    # print("end,", str(msg))

if __name__ == '__main__':
    date = '2021-06-08'
    # do_job(date)
    step = 3

    test_result = [(33, '08:00~10:00', 7), (33, '10:00~12:00', 3), (33, '12:00~14:00', 22),
                   (51, '06:00~08:00', 66), (198, '06:00~08:00', 7),
                   (198, '08:00~10:00', 10), (198, '10:00~12:00', 104), (224, '02:00~04:00', 5),
                   (249, '08:00~10:00', 25), (249, '12:00~14:00', 6), (266, '12:00~14:00', 8),
                   (266, '14:00~16:00', 3), (266, '16:00~18:00', 18), (272, '06:00~08:00', 11),
                   (272, '08:00~10:00', 6), (272, '10:00~12:00', 50),
                   ]

    pool = Pool(processes=2)
    lock = Manager().Lock()

    length = len(test_result)
    for i in range(4):
        pool.apply_async(func, (i, lock,date))  # 使用元祖类型传参

    print("main function pid is " + str(os.getpid()))
    pool.close()  # 关闭进程池，不再接受新的进程
    pool.join()



'''
def func(msg):
    print("msg:", msg)
    time.sleep(3)
    print("end,", msg)

if __name__ == "__main__":
    # 这里设置允许同时运行的的进程数量要考虑机器cpu的数量，进程的数量最好别小于cpu的数量，
    # 因为即使大于cpu的数量，增加了任务调度的时间，效率反而不能有效提高
    pool = multiprocessing.Pool(processes = 3)
    item_list = ['processes1' ,'processes2' ,'processes3' ,'processes4' ,'processes5' ,]
    count = len(item_list)
    for item in item_list:
        msg = "hello %s" %item
        # 维持执行的进程总数为processes，当一个进程执行完毕后会添加新的进程进去
        pool.apply_async(func, (msg,))

    pool.close()
    pool.join()
'''












