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


'''自定义日志配置字典'''
'''
#使用示例
config.dictConfig(log_config)
crawler = logging.getLogger('crawler')
'''
log_config = {
    'version': 1.0,
    'formatters': {
        'detail': {
            # 'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'format': '%(asctime)s - %(message)s',
            'datefmt': "%Y-%m-%d %H:%M:%S" #如果不加这个会显示到毫秒。
        },
        'simple': {
            'format': '%(name)s - %(levelname)s - %(message)s',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',#日志打印到屏幕显示的类。
            'level': 'INFO',
            'formatter': 'detail'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',#日志打印到文件的类。
            'maxBytes': 1024 * 1024 * 10,             #单个文件最大内存
            'backupCount': 10,                       #备份的文件个数
            'filename': "logs/logger_test.txt", #日志文件名
            'level': 'INFO',# 日志等级
            'formatter': 'detail', #调用formatters的哪个格式
            'encoding': 'utf-8', #编码
        }
    },
    'loggers': {
        'debug': {
            'handlers': ['console', 'file'],                #打印到屏幕和写入文件
            'level': 'DEBUG',                               #只显示错误的log
        },
        'other': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
        },
        'file': {
            'handlers': ['file'],
            'level': 'INFO',
        }
    }
}

'''改变logger,改变输出日志目的地'''
'''
#使用示例后
使用新logger前，调用remove函数去除上一次使用的handler
_logger.removeHandler(logger.handlers[0])
'''
def setLogName(_logger,filename):
    _logger.setLevel(level=logging.INFO)

    handler = logging.FileHandler(filename, mode='w')
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    _logger.addHandler(handler)



'''返回一个经过设置的logger,每个进程可以声明一次'''
def get_logger(filename):
    '''

    :param _logger: logging.getLogger()
    :param filename: 日志路径
    :return:
    '''
    _logger = logging.getLogger()
    _logger.setLevel(level=logging.INFO)

    #handler
    handler = logging.FileHandler(filename, mode='a')
    handler.setLevel(logging.INFO)
    #日志格式
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)

    _logger.addHandler(handler)
    return _logger



'''日志文件路径设置,一个模块只能声明一次'''
def set_logger(logpath=''):
    log_config['handlers']['file']['filename'] = 'logs/'+logpath
    config.dictConfig(log_config)
    report = logging.getLogger('file')
    return report


