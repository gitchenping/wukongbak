import requests
import time
import random
import os, sys
import hashlib

# linux usage:python downshuxiang.py http://shuxiang.chineseall.cn/v3/book/read/Mu4yg/pdf/ 326

# bookurl='http://shuxiang.chineseall.cn/v3/book/content/isCmg/pdf/'
# totalpage=326
# cookies={'_Tvt5MJ89bV_':'605E52501D5E3259C8B628C3A95B7B3DB143DAAE10C929CE9CA9D88DB5991EA6FFCAF2B47CD0A00F1F9B6E34EFE097E19B14A0EE5D082FBE891C6FAE0253BFCC'}


def login(url,totalpage):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36', \
        'Host': 'shuxiang.chineseall.cn',
        'Referer': 'http://shuxiang.chineseall.cn/sso/login.jsps?redirectUrl=http://shuxiang.chineseall.cn', \
        'Content-Type': 'application/x-www-form-urlencoded'}

    # login
    s = requests.Session()
    # userpass = hashlib.md5(password).hexdigest()
    userpass = '1811171f9458bd64bda4e7e4acd43e93'
    data = {"userName": "tschenping", "userPass": userpass, "redirectUrl": "http://shuxiang.chineseall.cn"}
    loginurl = 'http://shuxiang.chineseall.cn/sso/ajaxLogin.action'

    r = s.post(loginurl, data=data)
    if len(sys.argv) >1:
        bookurl = sys.argv[1]
        totalpage = int(sys.argv[2])
    else:
        bookurl = url


    bookurl = bookurl.replace('read', 'content')
    bookurl = bookurl.replace('PDF', 'pdf')

    return s,bookurl,totalpage


def progressbar(processnum,totalnum):
    scale=int(processnum / totalnum *100)
    j = '>'*scale
    sys.stdout.write('\r')
    sys.stdout.write('【' + j + '】->' + str(scale) + "%")
    sys.stdout.flush()


if __name__ == '__main__':
    if os.name != "posix":
        windows_url ='http://shuxiang.chineseall.cn/v3/book/read/Q8TJg/PDF/'
        totalpage = 190
    else:
        windows_url = sys.argv[1]
        totalpage = sys.argv[2]

    s,bookurl,totalpage = login(windows_url,totalpage)
    try:
        for i in range(1, totalpage + 1):
            delay = random.randint(1, 10) * 0.3

            time.sleep(delay)

            atime = time.time() * 1000
            requesttime = str(atime).split('.')[0]

            # rtext=requests.get(bookurl+str(i)+'?t='+str(requesttime),headers=headers,cookies=cookies)
            rtext = s.get(bookurl + str(i) + '?t=' + str(requesttime))

            with open('resource/'+str(i) + '.jpg', 'wb') as f:
                f.write(rtext.content)

            progressbar(i, totalpage)
    except Exception as e:
        print(e)
    finally:

        print("")
        # logout
        s.get('http://shuxiang.chineseall.cn/sso/logout.jsps')