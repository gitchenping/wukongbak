import requests
import time
import random
import os, sys
import hashlib
import pdfkit

# linux usage:python downshuxiang.py http://shuxiang.chineseall.cn/v3/book/read/Mu4yg/pdf/ 326

# bookurl='http://shuxiang.chineseall.cn/v3/book/content/isCmg/pdf/'
# totalpage=326
# cookies={'_Tvt5MJ89bV_':'605E52501D5E3259C8B628C3A95B7B3DB143DAAE10C929CE9CA9D88DB5991EA6FFCAF2B47CD0A00F1F9B6E34EFE097E19B14A0EE5D082FBE891C6FAE0253BFCC'}


def login():
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
    return s


def progressbar(processnum,totalnum):
    scale=int(processnum / totalnum *100)
    j = '>'*scale
    sys.stdout.write('\r')
    sys.stdout.write('【' + j + '】->' + str(scale) + "%")
    sys.stdout.flush()


if __name__ == '__main__':

    if os.name != "posix":
        book_url ='http://shuxiang.chineseall.cn/v3/book/read/Ng2Nj/EPUB/'
        # book_url ='http://shuxiang.chineseall.cn/v3/book/read/Q8TJg/PDF/'
        totalpage = 28
    else:                        #linux 平台
        book_url = sys.argv[1]
        totalpage = sys.argv[2]

    s = login()


    try:
        bookurl = book_url.replace('read', 'content')

        pdf = 0  #标志
        file_path = "resource/"

        if book_url.__contains__('PDF'):

            bookurl = bookurl.replace('PDF', 'pdf')
            file_suffix = ".jpg"
            pdf = 1
        else:
            bookurl = bookurl.replace('EPUB', 'epub')
            file_suffix= ".html"

        for i in range(1, totalpage + 1):
            delay = random.randint(1, 10) * 0.3
            time.sleep(delay)

            atime = time.time() * 1000
            requesttime = str(atime).split('.')[0]
            # rtext=requests.get(bookurl+str(i)+'?t='+str(requesttime),headers=headers,cookies=cookies)
            rtext = s.get(bookurl + str(i) + '?t=' + str(requesttime))


            with open(file_path+str(i) + file_suffix, 'wb') as f:
                if pdf == 0:
                    f.write(b'<head><link href="epub/style_css/page_styles.css"  type="text/css"/></head>')
                f.write(rtext.content)

            progressbar(i, totalpage)
    except Exception as e:
        print(e)
    finally:

        #对 html 文件进行转换
        config = pdfkit.configuration(wkhtmltopdf='D:\\Program Files (x86)\\wkhtmltopdf\\bin\\wkhtmltopdf.exe')
        options = {
            'page-size': 'A4',
            'margin-top': '0.8in',
            'margin-right': '0.6in',
            'margin-bottom': '0.6in',
            'margin-left': '0.5in',
            'encoding': "UTF-8",
        }

        html_file_list = os.listdir(file_path)
        for each_file in html_file_list:
            name,name_type = os.path.splitext(each_file)
            pdfkit.from_file(configuration= config,options = options,
                             input = file_path+each_file,output_path='epub/'+name+".pdf",
                             css = 'epub/style_css/page_styles.css')

        print("")
        # logout
        s.get('http://shuxiang.chineseall.cn/sso/logout.jsps')