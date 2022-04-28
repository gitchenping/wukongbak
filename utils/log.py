import logging.handlers
from logging import config
import sys,time

'''详细日志记录'''
# logging初始化工作
'''
filetime=time.strftime('%m-%d-%H', time.localtime())
myapp = logging.getLogger()
myapp.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s==> %(message)s')
filehandler = logging.handlers. RotatingFileHandler(filetime+"detail_log.txt", mode='a', maxBytes=1024*1024*10,backupCount=10)#20M,分文件大小
filehandler.setFormatter(formatter)
myapp.addHandler(filehandler)
def log(*args,**kwargs):
    myapp.info(*args,**kwargs)
'''


#自定义日志配置字典
LogConfig = {
    'version': 1.0,
    'loggers': {
        'console': {                                         #写控制台
            'handlers': ['hconsole'],
            'level': 'DEBUG',
        },
        'file': {                                           #写文件
            'handlers': ['hfile'],
            'level': 'INFO',
        },
        'tfile':{
            'handlers': ['htfile'],
            'level': 'INFO',
        },
        'other': {
            'handlers': ['hconsole', 'hfile'],        #打印到屏幕和写入文件
            'level': 'INFO',
        }

    },
    'formatters': {
        'only_msg':{
            'format': '%(message)s'
        },
        'simple': {
            'format': '%(name)s - %(levelname)s - %(message)s',
        },
        'detail': {
            # 'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'format': '%(asctime)s - %(message)s',
            'datefmt': "%Y-%m-%d %H:%M:%S"      #如果不加这个会显示到毫秒
        }

    },
    'handlers': {
        'hconsole': {
            'class': 'logging.StreamHandler',               #日志打印到屏幕显示的类
            'level': 'INFO',
            'formatter': 'only_msg'
        },
        'hfile': {
            'class': 'logging.handlers.RotatingFileHandler',#日志打印到文件的类(按文件大小）
            'maxBytes': 1024 * 1024 * 10,             #单个文件最大内存
            'backupCount': 10,                       #生成文件个数，超过覆盖
            'filename': "unknown.txt",         #默认日志文件名(主程序所在目录）
            'level': 'INFO',                # 日志等级
            'formatter': 'only_msg',       #调用formatters的哪个格式
            'encoding': 'utf-8',        #编码
        },
        'htfile':{
            'class': 'logging.handlers.TimedRotatingFileHandler',#日志打印到文件的类(按时间切割）
            'when': "M",                        #S:秒、M：分、H：时、D：天、W：周
            'interval': 1,                       #滚动周期，单位有when指定，比如：when='D',interval=1，表示每天产生一个日志文件
            'backupCount':10,
            'filename': "unknown.txt",         #默认日志文件名(主程序所在目录）
            'level': 'INFO',                # 日志等级
            'formatter': 'only_msg',       #调用formatters的哪个格式
            'encoding': 'utf-8',        #编码
        }
    }

}


'''设置日志logger'''
def set_logger(logclass = 'console',filename = '',formatter = ''):

    '''
    :param logclass: logger类
    :param filename: 日志文件名字,默认run.py同级目录log下
    :return:
    '''
    h_handler = 'h'+logclass

    if logclass != 'console':   #非控制台打印
        if filename != '':
            LogConfig['handlers'][h_handler]['filename'] = './logs/'+filename

    if formatter !='':
        LogConfig['handlers'][h_handler]['formatter'] = formatter

    config.dictConfig(LogConfig)
    reporter = logging.getLogger(logclass)
    return reporter



'''获取一个logger,每个进程可以声明一次'''
def get_logger(_logger = None,filename = '',formatter = '%(message)s'):
   '''

   :param _logger: 全局或局部logger,使用全局logger时，应删除前面创建的handler
   调用remove函数去除上一次使用的handler：_logger.removeHandler(logger.handlers[0])
   :param filename:
   :param formatter:
   :return: logger对象
   '''
   if _logger is None:  # 否则使用外部全局logger
       _logger = logging.getLogger('')
       _logger.setLevel(level=logging.INFO)

   if filename == '':  #控制台显示
       handler = logging.StreamHandler(sys.stdout)
   else:
       handler = logging.FileHandler(filename, mode='a')

   # formatter
   formatter = logging.Formatter(formatter)
   handler.setFormatter(formatter)
   handler.setLevel(logging.INFO)

   _logger.addHandler(handler)
   return _logger





