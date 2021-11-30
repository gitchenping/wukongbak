'''
评书吧 音频下载
'''
import requests
import time
import random

PAGE_NUM=56
DOWNLOAD_PAGE_BEGIN=218139

BASE_URL="http://www.pingshu8.com/"
#header
user_agent="Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"



for i in range(0,PAGE_NUM):
        pagenum=i+DOWNLOAD_PAGE_BEGIN
        DOWNLOAD_PAGE="down_"+str(pagenum)+".html"
        DOWNLOAD_URL=BASE_URL+DOWNLOAD_PAGE

        referer=DOWNLOAD_URL

        heads={"User-Agent":user_agent,"Referer":referer}
        time.sleep(3*random.random())

        downurl='http://www.pingshu8.com/bzmtv_Inc/download.asp?fid='+str(pagenum)+'&t='+str(time.time())
        r=requests.get(downurl,headers=heads,allow_redirects=True)

        with open('/newtest/'+str(i)+'.mp3','wb') as fp:
                fp.write(r.content)