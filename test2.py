from concurrent import futures
import time
import random
import happybase
from concurrent.futures._base import Future
from utils import log
from multiprocessing import Manager

conn = happybase.Connection(host='10.5.24.53', port=9090, timeout=10000)
# visit habse
table = happybase.Table('ddse_product_clonefromsnapshot', conn)

report = log.set_logger('test.txt')

def task(line,lock):
    # print('total '+str(workers)+" workers")
    a= line*2
    return line,a,lock


def afterfun(res):
    r=res.result()

    # r[-1].acquire()
    report.info('calcaute ele '+str(r[0]))
    report.info(r[1])
    # r[-1].release()



def dotask():

    workers=2
    lock = Manager().Lock()
    with futures.ProcessPoolExecutor(workers) as executor:
            for each_line in [1,2,3,4,5]:
                future = executor.submit(task, each_line,lock)
                future.add_done_callback(afterfun)

        # for ele in tasklist:
        #     future = executor.submit(task,ele)
        #     future.add_done_callback(afterfun)

    print('main thread done')

if __name__ == '__main__':
    dotask()