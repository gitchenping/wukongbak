'''
装饰器函数
'''

import configparser
import os
import logging
from utils.load import readconfini


'''环境变量预加载'''
def loadenv(**kwargs):
    env = readconfini()

    host=env.get(kwargs['db'],'db_host')
    port=env.get(kwargs['db'],'db_port')
    user=env.get(kwargs['db'],'db_username')
    password=env.get(kwargs['db'],'db_password')
    database = env.get(kwargs['db'],'db_name')

    def wrap_o(func):
        def wrap(**args):       #如果函数带参数，使用实参
            nonlocal host
            nonlocal port
            nonlocal user
            nonlocal password
            nonlocal database

            #host
            if args.__contains__('host') and args['host'] is not None:
                host = args['host']

            # port
            if args.__contains__('port') and args['port'] is not None:
                port = args['port']

            #user
            if args.__contains__('user') and args['user'] is not None:
                user = args['user']

            #password
            if args.__contains__('password') and args['password'] is not None:
                password = args['password']

            #database
            if args.__contains__('database') and args['database'] is not None:
                database = args['database']

            #collection
            if args.__contains__('collection') and args['collection'] is not None:
                collection=args['collection']
            else:
                if env.has_option(kwargs['db'],'db_collection'):
                    collection = env.get(kwargs['db'],'db_collection')
                else:
                    collection = None
            return func(host,port,user,password,database,collection)
        return wrap
    return wrap_o
    pass

#数据库环境加载2
def loaddbenv(**kwargs):
    env = readconfini()
    host = env.get(kwargs['db'], 'db_host')
    port = env.get(kwargs['db'], 'db_port')
    user = env.get(kwargs['db'], 'db_username')
    password = env.get(kwargs['db'], 'db_password')
    database = env.get(kwargs['db'], 'db_name')

    def wrap_o(func):
        def wrap_in(self,**args):       #如果函数带参数，使用实参
            nonlocal host
            nonlocal port
            nonlocal user
            nonlocal password
            nonlocal database

            #host
            if args.__contains__('host') and args['host'] is not None:
                host=args['host']

            # port
            if args.__contains__('port') and args['port'] is not None:
                port = args['port']

            #user
            if args.__contains__('user') and args['user'] is not None:
                user = args['user']

            #password
            if args.__contains__('password') and args['password'] is not None:
                password = args['password']

            #database
            if args.__contains__('database') and args['database'] is not None:
                database=args['database']

            #collection
            if args.__contains__('collection') and args['collection'] is not None:
                collection=args['collection']
            else:
                if env.has_option(kwargs['db'],'db_collection'):
                    collection = env.get(kwargs['db'],'db_collection')
                else:
                    collection = None
            func(self,host,port,user,password,database,collection)
        return wrap_in
    return wrap_o
    pass

#日志记录1
def complog(logger = None):
    def decorator(func):
        def wrapper(*args, **kw):

            result=func(*args, **kw)
            filter = args[0]    # 原函数第一个参数作为比较条件
            if logger is not None and result!={}:

                logger.info('比对条件: '+str(filter)+"-*-Fail-*-")
                logger.info("diff info:" + str(result))
                logger.info(' ')
            else:
                logger.info('比对条件: ' + str(filter) + "-*-Pass-*-")
            # return result
        return wrapper
    return decorator

#日志记录2
def logrecord(logger = None):
    def decorator(func):
        def wrapper(*args, **kw):
            where_result_ge = func(*args, **kw)  #原函数是一个生成器
            if logger is not None:
                color_start = '';
                color_end = ''
                if not isinstance(logger.handlers[0],logging.FileHandler):
                    color_start ='\033[1;31m' ;color_end ='\033[0m'
                for ge in where_result_ge:
                    msg = '筛选条件: ' + ge[0]
                    diffvalue = ge[1]

                    if diffvalue != {}:
                        logger.warning(msg +"1;"+color_start+ " -*-Fail-*-"+color_end)
                        logger.info(diffvalue)
                    else:
                        logger.info(msg + " -*-Pass-*-")
                    logger.info(' ')
            else:
                print("no logger !!!")
        return wrapper
    return decorator


#api请求重试
def retry(maxretry=3):
    def decorator(func):
        def wrapper(*args, **kw):
            att = 0
            while att < maxretry:
                try:
                    return func(*args, **kw)
                except Exception as e:
                    att += 1
        return wrapper
    return decorator


import functools

'''权限校验'''
def authenticate(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        request = args[0]
        if check_user_logged_in(request):  # 检查用户是否登录
            return func(*args, **kwargs)
        else:
            raise Exception('Authentication failed')  # 否则，身份验证失败
    return wrapper