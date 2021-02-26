import requests
import json
from utils import util,_sql
from web import sql,api
from utils.recommend_map import *

count=0
for bd_key, bd_value in bd_name.items():
    pathname_list = path2_name[bd_key]
    for pathname in pathname_list:

        for page_key, page_value in page_name.items():
            module_list = module_name[page_key]
            for module in module_list:
                count+=1
print(count)