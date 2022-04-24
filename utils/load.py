import configparser
import os

def readini(path):
    cf=configparser.ConfigParser()
    cf.read(path,encoding='utf-8')
    return cf

#读配置文件conf/config.ini
def readconfini():
    current_file_abspath = os.path.abspath(__file__)
    father_path= os.path.dirname(os.path.dirname(current_file_abspath))
    full_path = os.path.join(father_path, "conf/config.ini")

    cf = configparser.ConfigParser()
    cf.read(full_path, encoding='utf-8')

    return cf
