'''
辰东小说下载
https://www.cdxiaoshuo.cc
'''
import re,requests,random,time
import os
from bs4 import BeautifulSoup

BASE_URL ="https://www.cdxiaoshuo.cc"
#header
user_agent="Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"


def download_text(begin_url):
    heads = {"User-Agent": user_agent}
    #标签内容提取
    url = begin_url
    dirname_url = os.path.dirname(begin_url)

    while True:
        content_text = requests.get(url,verify=False)

        if content_text.status_code == 200:
            content_text.encoding = 'gbk'
            sp = BeautifulSoup(content_text.text, 'html.parser')
            rawtext = sp.select('#articlecontent')[0]
            text_content = rawtext.getText()
            text = text_content.replace('\xa0', '')

            # write file
            with open('resource/xiaoshuo.txt', 'a',encoding='utf-8') as f:
                f.write(text)

            next_page_info = sp.select('.nr_page')[0]
            next_page = next_page_info.select('a')[-1]
            if next_page.getText() == '下一章':
                a = next_page.get('href')
                if a is not None:
                    basename = os.path.basename(a)
                    if basename == '0.html':
                        break
                    url = dirname_url + "/" + basename
                    time.sleep(random.randint(1, 5) * 0.5)
                else:
                    break
            else:
                break
        else:
            break
    pass



if __name__=="__main__":

    #初始url
    begin_url = "https://www.cdxiaoshuo.cc/46/46520/11212614.html"

    download_text(begin_url)
