# coding=utf-8
'''
根据 api 递归获取页面所有可能输入参数
'''
from utils.apihandle import ApiHandle

def test(url,levelparentid):
    '''
    根据初始值 递归获取平台各级id
    usage:LevelParentid = [(2,'00.00.00'),(2,'01.00.00'),(2,'02.00.00')]
         alist = []
         test(url,LevelParentid)
    :param url:
    :return:
    '''
    for level_parentid in levelparentid:
        request_url = url.format(level = level_parentid[0] ,parentid = level_parentid[1])
        rjson = apihandle.get(request_url)

        platformsource_list = rjson['payload']['list']
        level = rjson['payload']['level']

        if level != '':
          level = int(level)

          if len(platformsource_list) > 0 :
              children_platformsource_list = [(level+1,ele['id']) for ele in platformsource_list]
              for ele in platformsource_list:
                  alist.append({ele['name']:[ele['source'],ele['platform'],ele['fromPlatform']]})

              test(url,children_platformsource_list)

          else:
              if level == 2:
                  alist.append(platformsource_list)


        pass

def test2(url,levelparentid):
    '''
    根据初始值 递归获取平台各级id
    :param data:
    :return:
    '''
    alist = []
    for level_parentid in levelparentid:
        request_url = url.format(level = level_parentid[0] ,parentid = level_parentid[1])
        rjson = apihandle.get(request_url)

        platformsource_list = rjson['payload']['list']
        level = rjson['payload']['level']

        if level != '':
          level = int(level)
          if len(platformsource_list) > 0 :
              children_platformsource_list = [(level+1,ele['id']) for ele in platformsource_list]
              for ele in platformsource_list:
                  alist.append({ele['name']:[ele['source'],ele['platform'],ele['fromPlatform']]})
              alist.extend(test(url,children_platformsource_list))
          else:
              if level == 2:
                  alist.extend([{'全部':platformsource_list}])
    return alist


def test3(url, levelparentid):
  '''
  根据初始值 递归获取品类各级id
  :param data:
  :return:
  '''

  alist = []
  for level_parentid in levelparentid:
    request_url = url.format(level=level_parentid[0], parentid=level_parentid[1])
    rjson = apihandle.get(request_url)

    platformsource_list = rjson['payload']['list']
    level = rjson['payload']['level']

    if level != '':
      level = int(level)
      if len(platformsource_list) > 0:
        children_platformsource_list = [(level + 1, ele['path']) for ele in platformsource_list]
        for ele in platformsource_list:
          alist.append({ele['name']: ele['path']})
        alist.extend(test3(url, children_platformsource_list))
      else:
        if level == 2:
          alist.extend([{'全部': platformsource_list}])
  return alist


token = "xx"

url_platform ="http://test.newwk.dangdang.com/api/v3/reportForm/platformList?level={level}&parentId={parentid}"
url_category ="http://test.newwk.dangdang.com/api/v3/reportForm/categoryList?level={level}&parentId={parentid}"

apihandle = ApiHandle(token = token)
platform_LevelParentid = [(2,'00.00.00'),(2,'01.00.00'),(2,'02.00.00')]
category_LevelParentid = [(2,'01.00.00.00.00.00')]
#platform
# alist = []
# test(url_platform,platform_LevelParentid)
# print(alist)

#category
a = test3(url_category,category_LevelParentid)
print(a)

