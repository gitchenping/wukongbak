
import threading



'''自定义线程类，实例化时传入一个函数及其参数'''
class myThread(threading.Thread):
    def __init__(self,func,*arg,**kwargs):
        '''

        :param func: task 函数
        :param arg:  task 包裹位置参数
        :param kwargs: task 包裹关键字参数
        '''
        threading.Thread.__init__(self)
        self.arg = arg
        self.kw = kwargs
        self.func = func

    def run(self):
        self.result= self.func(*self.arg,**self.kw)

    #返回值
    def get_result(self):
        try:
            return self.result
        except Exception:
            return None