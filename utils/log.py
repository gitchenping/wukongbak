import logging.handlers
from logging import config
import time

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


'''自定义日志配置字典

#使用示例
config.dictConfig(log_config)
crawler = logging.getLogger('file')
'''
LogConfig = {
    'version': 1.0,
    'loggers': {
        'debug': {
            'handlers': ['console', 'file'],                #打印到屏幕和写入文件
            'level': 'DEBUG',                               #只显示错误的log
        },
        'other': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
        },
        'file': {                                           #写文件
            'handlers': ['file'],
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
        'console': {
            'class': 'logging.StreamHandler',               #日志打印到屏幕显示的类
            'level': 'INFO',
            'formatter': 'detail'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',#日志打印到文件的类
            'maxBytes': 1024 * 1024 * 10,             #单个文件最大内存
            'backupCount': 10,                       #备份的文件个数
            'filename': "unknown.txt",         #默认日志文件名(主程序所在目录）
            'level': 'INFO',                # 日志等级
            'formatter': 'only_msg',       #调用formatters的哪个格式
            'encoding': 'utf-8',        #编码
        }
    }

}


'''设置日志logger'''
def set_logger(filename = '',formatter = ''):
    '''

    :param log_name: 日志文件名字,默认run.py同级目录log下
    :return:
    '''
    if filename != '':
        LogConfig['handlers']['file']['filename'] = './logs/'+filename
    if formatter !='':
        LogConfig['handlers']['file']['formatter'] = formatter


    config.dictConfig(LogConfig)
    report = logging.getLogger('file')
    return report



'''获取一个logger,每个进程可以声明一次'''
def get_logger(_logger = None,filename = '',formatter = '%(message)s'):
   '''

   :param _logger: 全局或局部logger,使用全局logger时，应删除前面创建的handler
   调用remove函数去除上一次使用的handler：_logger.removeHandler(logger.handlers[0])
   :param filename:
   :param formatter:
   :return: logger对象
   '''
   if _logger is None:      #否则使用外部全局logger
       _logger = logging.getLogger('')
       _logger.setLevel(level=logging.INFO)

   # formatter
   formatter = logging.Formatter(formatter)
   if filename == '':
        filename = './log.txt'

   #handler
   handler = logging.FileHandler(filename, mode='a')
   handler.setLevel(logging.INFO)
   handler.setFormatter(formatter)
   _logger.addHandler(handler)

   return _logger





