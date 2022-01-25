import configparser
import os
from utils.load import readconfini

#装饰器函数


'''环境变量预加载'''
def loadenv(**kwargs):
    env = readconfini('./conf/')
    # host=env.get(kwargs['db'],'db_host')
    # port=env.get(kwargs['db'],'db_port')
    # user=env.get(kwargs['db'],'db_username')
    # password=env.get(kwargs['db'],'db_password')

    def wrap_o(func):
        def wrap(**args):       #如果函数带参数，使用实参

            #host
            if args.__contains__('host') and args['host'] is not None:
                host=args['host']
            else:
                host = env.get(kwargs['db'], 'db_host')

            # port
            if args.__contains__('port') and args['port'] is not None:
                port = args['port']
            else:
                port = int(env.get(kwargs['db'], 'db_port'))

            #user
            if args.__contains__('user') and args['user'] is not None:
                user = args['user']
            else:
                user = env.get(kwargs['db'], 'db_username')

            #password
            if args.__contains__('password') and args['password'] is not None:
                password = args['password']
            else:
                password = env.get(kwargs['db'], 'db_password')

            #database
            if args.__contains__('database') and args['database'] is not None:
                database=args['database']
            else:
                database = env.get(kwargs['db'], 'db_name')

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

'''日志记录'''
def complog(logger = None):
    def decorator(func):
        def wrapper(*args, **kw):

            result=func(*args, **kw)
            filter = args[0]  # 第一个参数作为比较条件
            if logger is not None and result!={}:

                logger.info('比对条件: '+str(filter)+"-*-Fail-*-")
                logger.info("diff info:" + str(result))
                logger.info(' ')
            else:
                logger.info('筛选条件: ' + str(filter) + "-*-Pass-*-")
            # return result
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