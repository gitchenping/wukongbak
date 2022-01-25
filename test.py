import asyncio
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from utils.db import connect_sqlserver



def _http(url):
    # arg = url.split('?')[1]
    # time.sleep(len(arg))
    # return arg
    res = requests.get(url)
    return res.text

async def async_http():


    loop = asyncio.new_event_loop()
    executor = ThreadPoolExecutor(2)
    tasks =[]
    for custid in [12,123]:
        url = 'http://10.255.255.31:8000/?a={}'.format(custid)
        tasks.append(loop.run_in_executor(executor,_http,url))

    return await asyncio.gather(*tasks)


def async_main():
    start = time.time()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(async_http())
    # loop.close()
    end = time.time()
    print('花费时间{}s'.format(end - start))


def _db(cursor,email):
    sql = "select custid,Email from Customers where Email like '%{}'".format(email)
    # time.sleep(len(email))
    # return len(email)
    cursor.execute(sql)
    return cursor.fetchall()



def aync_db(executor):
    _, cursor = connect_sqlserver('customer')
    start = time.time()
    loop = asyncio.get_event_loop()
    # loop = asyncio.get_running_loop()
    tasks = []
    for email in ['22cn.com', '264.net']:
        sql = "select custid,Email from Customers where Email like '%{}'".format(email)
        # temp = async_db(loop, cursor,email)
        # task = loop.create_task(temp)
        task  = loop.run_in_executor(executor, _db,cursor, email)
        tasks.append(task)

    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
    end = time.time()
    for task in tasks:
        print("task ret:", task.result())
    print('花费时间{}s'.format(end - start))



def sync_http():
    start = time.time()

    for custid in [12,134]:
        # url = "http://10.255.255.99:8100/customer/loginapi/get_user_viptype.php?custid={}".format(custid)
        url = 'http://10.255.255.31:8000/?a={}'.format(custid)
        res = requests.get(url)
        print("ret :"+res.text)
    end = time.time()
    print('花费时间{}s'.format(end - start))




# aync_db(executor)
async_main()
# sync_http()


