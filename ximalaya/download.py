import requests
import json
import sys
import os
import time
import re
from requests.packages import urllib3
urllib3.disable_warnings()

#默认使用vip下载
VIP = True
if os.name == "posix":
    input_arg = sys.argv  #输入搜索关键字参数
    if len(input_arg) < 2:
        print("args is wrong,usage:python3 myscript.py search_word vip_flag")
        exit(0)
    else:
        search_keyword = input_arg[1]
        if len(input_arg) == 3:
            temp = input_arg[2]
            if temp not in ['True','False']:
                print("vip type is wrong,only support True or False")
                exit(0)
            VIP = eval(temp)

else:
    VIP = False
    search_keyword = '多多读报'


def progressbar(processnum,totalnum):
    scale=int(processnum / totalnum *100)
    j = '>'*scale
    sys.stdout.write('\r')
    sys.stdout.write('【' + j + '】->' + str(scale) + "%")
    sys.stdout.flush()

def get_resource_list(vip):
    # headers
    useragent = "ting_6.6.21(Redmi+K30+5G+Speed,Android29)"
    host = "audiopay.cos.tx.xmcdn.com"

    cookies = ''
    headers = {"user-agent": useragent, "Host": host, 'cookie': cookies}
    # cookies 字符串中不能有空格、换行
    cookies = "1&_device=android&654a4727-11c8-3cbf-8263-eec0d985e68a&8.3.3;1&_token=195711234&17BAB6905A5612FBA9B479F05E49D088cDv2B111CD57D6752A1A1B1446D79134130C64DBA9EBB2B03E23940DBAB77DC7ADE;channel=and-d10;impl=com.ximalaya.ting.android;osversion=29;fp=00311355412122b2vv349159180023a1232112711041000;device_model=Redmi+K30+5G+Speed;XIM=;c-oper=%E4%B8%AD%E5%9B%BD%E7%A7%BB%E5%8A%A8;net-mode=WIFI;freeFlowType=0;res=1080%2C2175;NSUP=42e8e1a3%2C421fd73b%2C1630979706141;AID=+f1X+JD6aJA=;manufacturer=Xiaomi;XD=G4gqXZCYTqJe2uGSHGiUwdGf8gbBpd2u+LptU/9Ro1vnB7hHXsIBBwM4uo2kB09iBUxHAdQVFowZ0IFtKFk6RrkirqRwEaWneq0oAc/qYZyhfHNSVb+aJlsjYWZumyIv;umid=654a472711c82cbf263eec0d985e68a;xm_grade=0;minorProtectionStatus=0;oaid=e2b5d3af13199dce;domain=.ximalaya.com;path=/;"
    headers['cookie'] = cookies
    # ximalaya
    PAGE = 1
    UID = 195711234


    search_url_prefix = 'http://search.ximalaya.com'
    search_url_suffix = "/front/v1?appid=0&categoryId=-1&condition=relation&core=album&device=android&" \
                        "deviceId=1e43bdd2-c7f5-33fb-b745-f9c3e291d5a2&fq=categoryId:-1&" + \
                        "kw={searchword}&live=true&network=wifi&operator=0&page=1&paidFilter=false&" \
                        "plan=c&recall=normal:group2&rows=20&search_version=2.6&" + \
                        "spellchecker=true&uid={userid}&version=6.6.21"
    # 搜索地址
    search_kw_url = search_url_prefix + search_url_suffix.format(searchword=search_keyword, userid=UID)

    # 可能的资源列表
    album_rawinfo = requests.get(search_kw_url).text
    album_rawinfo_json = json.loads(album_rawinfo)

    albuminfo = album_rawinfo_json['response']['docs']

    # list all resources
    search_resource_list = []
    if len(albuminfo) > 0:
        print('find relating resources listed below:')
        i = 1
        for ele in albuminfo:
            resource_name = ele['title']
            resource_zhubo = ele['nickname']
            info = str(i) + '、名称:' + resource_name + '  主播:' + resource_zhubo
            i += 1
            print(info)
    else:
        print("bad search,no resource find!")

    choice_num = input("which resource would you like to download,input the index number:")
    resource_selected = albuminfo[int(choice_num) - 1]

    # 资源的集数、albumId
    tracks_num = resource_selected['tracks']
    if vip:
        albumid = resource_selected['price_types'][0]['id']
    else:
        albumid = resource_selected['id']

    # 获取资源下各集的名字
    starttime = str(int(round(time.time() * 1000)))
    url_for_resource_name = "http://mobile.ximalaya.com/mobile/v1/album/track/ts-{}?" \
                            "albumId={}&device=android&isAsc=true&isQueryInvitationBrand=true&" \
                            "pageId=1&pageSize={}&pre_page=0".format(starttime, albumid, tracks_num)
    r = requests.get(url_for_resource_name)

    rjson = json.loads(r.text)
    resourcelist = rjson['data']['list']

    each_resource_name_list = [ele['title'] for ele in resourcelist]
    audio_url_list = []
    if not vip:

        for ele in resourcelist:
            if ele.__contains__('playUrl32') and ele['playUrl32'] !='':
                audio_url_list.append(ele['playUrl32'])
            else:
                audio_url_list.append(ele['playUrl64'])

    return each_resource_name_list,audio_url_list
    pass

def download_audio(vip = False):

    resource_name_list,audio_url_list = get_resource_list(vip)

    print('专辑资源名称列表:')
    i = 0
    for ele in resource_name_list:
        print(str(i)+"、"+ele)
        i+=1
    choice_yes_or_no = input("are you sure to go on [y/yes/n/no]:")
    if choice_yes_or_no.lower() in ['n','no']:
        return


    file_name_suffix =".mp3"
    headers = None
    if vip:
        # headers
        useragent = "ting_6.6.21(Redmi+K30+5G+Speed,Android29)"
        host = "audiopay.cos.tx.xmcdn.com"
        cookies = ''
        headers = {"user-agent": useragent, "Host": host, 'cookie': cookies}
        # cookies 字符串中不能有空格、换行
        cookies = "1&_device=android&654a4727-11c8-3cbf-8263-eec0d985e68a&8.3.3;1&_token=195711234&17BAB6905A5612FBA9B479F05E49D088cDv2B111CD57D6752A1A1B1446D79134130C64DBA9EBB2B03E23940DBAB77DC7ADE;channel=and-d10;impl=com.ximalaya.ting.android;osversion=29;fp=00311355412122b2vv349159180023a1232112711041000;device_model=Redmi+K30+5G+Speed;XIM=;c-oper=%E4%B8%AD%E5%9B%BD%E7%A7%BB%E5%8A%A8;net-mode=WIFI;freeFlowType=0;res=1080%2C2175;NSUP=42e8e1a3%2C421fd73b%2C1630979706141;AID=+f1X+JD6aJA=;manufacturer=Xiaomi;XD=G4gqXZCYTqJe2uGSHGiUwdGf8gbBpd2u+LptU/9Ro1vnB7hHXsIBBwM4uo2kB09iBUxHAdQVFowZ0IFtKFk6RrkirqRwEaWneq0oAc/qYZyhfHNSVb+aJlsjYWZumyIv;umid=654a472711c82cbf263eec0d985e68a;xm_grade=0;minorProtectionStatus=0;oaid=e2b5d3af13199dce;domain=.ximalaya.com;path=/;"
        headers['cookie'] = cookies

        file_name_suffix = ".m4a"
        # 手动获取各资源下载地址,charles file export sessions
        audio_url_list = []
        # with open('resource/Untitled.csv', 'r') as fp:
        #     for line in fp:
        #         url = line.split(',')[0]
        #         if '.m4a' in url:
        #             audio_url_list.append(url)

        with open('resource/audio.txt', 'r') as fp:
            for line in fp:
                url = line.strip('\n')
                if '.m4a' in url:
                    if url not in audio_url_list: #去重
                        audio_url_list.append(url)

    len_audio_url_list =len(audio_url_list)
    len_resource_name = len(resource_name_list)
    print("there are total " + str(len_resource_name) + " 集"+"，and audio "+str(len_audio_url_list)+"集")
    if len_audio_url_list != len_resource_name:
        choice_yes_or_no = input("number not match !!!  go on or not 【y/yes/n/no】:")
        if choice_yes_or_no.lower() not in ['y', 'yes']:
            return
        else:
            choice = input("please input a new starting number to go:")
            resource_name_list = resource_name_list[int(choice):]
    else:
        choice_start = input("please input a start number to go:")
        choice_end = input("please input an end number:")

        resource_name_list = resource_name_list[int(choice_start):int(choice_end)+1]
        audio_url_list = audio_url_list[int(choice_start):int(choice_end)+1]

    print("now begin downloading......")

    i = 1
    for resource_name, url in zip(resource_name_list, audio_url_list):
        # cmd = "curl -s -o '" + resource_name.encode(
        #     'utf-8') + ".m4a' " + '-H "user-agent:' + useragent + '"' + ' -H "Host:' + host + '"' + " -b loginmobile.txt '" + url + "'"
        # ret = os.popen(cmd)
        r = requests.get(url, headers=headers, verify=False)
        resource_name = re.sub('[*/\?"\']', '',resource_name)
        with open('resource/'+resource_name+file_name_suffix, 'wb') as f:
            f.write(r.content)

        progressbar(i, len_audio_url_list)
        i += 1
    print('')





if __name__ == '__main__':
    vip = VIP

    download_audio(vip)

