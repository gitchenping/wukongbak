'''
推荐新品置顶
'''
import re,json
import happybase
import requests
from utils.db import connect_mysql_from_jump_server,connect_mysql

def get_all_data(creation_date):
    sql = '''select 
rank_result,product_id,_category_path
from(
SELECT
case when @cat != book.category_path then @rank:=1
else @rank:=@rank+1
end as rank_result,
@cat := book.category_path as _category_path,
product_id
from 
`nbdp_bang`.bang_booknew as book,
(select @cat := '') as t1,
(select @rank:= 0) as t2
where creation_date = "{creation_date}" 
and category_path in ('01.03.00.00.00.00','01.21.00.00.00.00','01.49.00.00.00.00') 
order by category_path,first_stock_date asc
) t
where rank_result <=500
'''
    #全部分类
    full_sql ='''SELECT 
@i:=@i+1 AS rank, product_id,'01.00.00.00.00.00' AS category_path
FROM `nbdp_bang`.bang_booknew, (SELECT @i:=0) b
WHERE creation_date = "{creation_date}" 
ORDER BY first_stock_date ASC
LIMIT 500
    '''

    sql_format = sql.format(creation_date=creation_date)
    full_sql_format = full_sql.format(creation_date=creation_date)

    # 连接mysql数据库
    MYSQL_HOST = 'mynbdpbangdbr.idc2'
    MYSQL_PORT = 3306
    MYSQL_USER = 'reader'
    MYSQL_PASSWD = 'n0p4ssw0rd'
    MYSQL_DATABASE = 'nbdp_bang'

    server, cursor = connect_mysql_from_jump_server(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWD, MYSQL_DATABASE)

    hbase_conn = happybase.Connection(host='10.5.24.53', port=9090, timeout=10000)
    # visit habse
    hbase_table = happybase.Table('ddse_product_clonefromsnapshot', hbase_conn)

    sql_data = {}

    for sql in [sql_format,full_sql_format][0:1]:
        cursor.execute(sql)
        rawdata = cursor.fetchall()
        for item in rawdata:
            path = item[2]
            product_id = str(item[1])

            rowkey = product_id[-3:] + product_id
            data = hbase_table.row(rowkey)
            if data == {}:
                continue

            sell_flag = data[b'a:sell_flag'].decode()
            if sell_flag in [1 ,'1']:
                if path not in sql_data:
                    sql_data[path] = [str(product_id)]
                else:
                    sql_data[path].append(str(product_id))

    return sql_data



def new_book(filename,creation_date):

    ic_key_re = re.compile('[0-9]{2}\..*\.[0-9]{2}')
    doc_info_re = re.compile('\d{3,8}')

    hbase_data = {}
    i = 0
    j = 0
    with open(filename,'r') as f:
        for line in f:
            if line.startswith('ic_key'):
                ic_key_value = ic_key_re.search(line)
                path = ic_key_value.group()
                i += 1
            if line.startswith('doc_info'):
                doc_info_value = doc_info_re.findall(line)
                j += 1
            if i >= 1 and j == 1:
                hbase_data[path] = doc_info_value
                i = 0
                j = 0

    sql_data = get_all_data(creation_date)

    #diff
    diff_keyvalue = {}
    for key,value in sql_data.items():
        if hbase_data.__contains__(key):
            if len(sql_data[key]) != len(hbase_data[key]):
                print('length is different:'+key)
                continue
            #逐个元素比较
            sql_hbase = zip(sql_data[key],hbase_data[key])
            for ele in sql_hbase:
                if ele[0] != ele[1]:
                    diff_keyvalue[key] = ele
                    continue
        else:
            print('hbase miss path:'+key)

    if diff_keyvalue != {}:
        for key in diff_keyvalue.keys():
            print(key)
            print(diff_keyvalue[key])
    pass


def new_book_add(cate_id_path_map_path,creation_date,last_changed_date):
    ic_key_re = re.compile('[0-9]{2}\..*\.[0-9]{2}')
    doc_info_re = re.compile('\d{3,8}')
    re_add = re.compile('productId:(.*?),startDate:(.*?),endDate:(.*?),')
    mysql_cursor = connect_mysql(host ='10.255.242.39',user ='mapi',password = 'mapi1234',database = 'mdd_cms_test')
    hbase_conn = happybase.Connection(host='10.5.24.53', port=9090, timeout=10000)
    # visit habse
    hbase_table = happybase.Table('ddse_product_clonefromsnapshot', hbase_conn)

    #读二级分类
    cat_id_map = {}
    with open('resources/cat_id.text', 'r') as f:
        for line in f:
            line = line.strip('\n').split('\t')
            key = line[1]
            value = line[0][0:5]+'.00'*4
            cat_id_map[key] = value

    #新加品 {'01.58.00.00.00.00': {'25836714'}}、{'58.31.00.00.00.00': {'60036328'}}
    sql ='''SELECT 
    id, page_id,floor_type, floor_data, 
    date_format(start_date, '%Y-%m-%d') as s_date,
    date_format(end_date,'%Y-%m-%d') as e_date,
    last_changed_date 
    FROM mdd_cms_test.`cms_new_book_floor`  
    WHERE `page_id` IN ( 
    SELECT id  
    FROM mdd_cms_test.cms_new_book_page  
    WHERE `status`=1)  
    AND `floor_type` ='book-shelf'  
    AND (`last_changed_date`>"{last_changed_date}")  
    ORDER BY `last_changed_date` ASC,`id` ASC;
    '''
    sql_format = sql.format(last_changed_date = last_changed_date)

    mysql_cursor.execute(sql_format)
    raw_sqldata = mysql_cursor.fetchall()
    #筛选符合上线条件的数据，并提取product_id
    product_id_list = []
    if len(raw_sqldata) >0:
        for item in raw_sqldata[0:]:
            s_date = item[-3]
            e_date = item[-2]
            json_floor_data = item[3]

            if (e_date is not None and creation_date > e_date) or (s_date is not None and creation_date < s_date):
                continue
            inner_info = re_add.findall(json_floor_data)
            for inner_item in inner_info:
                product_id = inner_item[0].strip('"')
                s_i_date = inner_item[1].strip('"')
                e_i_date = inner_item[2].strip('"')
                if product_id != '':
                    if ( e_i_date !='null' and creation_date > e_i_date) or (s_i_date !='null' and creation_date < s_i_date):
                        continue
                    product_id = product_id.strip('"')
                    product_id_list.append(product_id)

        #由上述product_id 去hbase 查cat_id
        add_sql_data = {}
        for product_id in product_id_list:

            rowkey = product_id[-3:]+product_id
            data = hbase_table.row(rowkey)
            if data == {}:
                continue
            # print(rowkey)
            catid = data[b'a:cat_id'].decode()
            sell_flag = data[b'a:sell_flag'].decode()

            #由catid查二级分类
            if sell_flag in [1,'1']:
                catgory_path = cat_id_map[catid]
                if catgory_path not in add_sql_data:
                    add_sql_data[catgory_path] = [str(product_id)]
                else:
                    add_sql_data[catgory_path].append(str(product_id))

    all_data = get_all_data(creation_date) #全量数据
    # 合并
    for cat in add_sql_data.keys():
        if all_data.__contains__(cat):

            for ele in add_sql_data[cat][::-1]:
                try:
                    index = all_data[cat].index(ele)
                    all_data[cat].pop(index)
                except ValueError:
                    pass
                finally:
                    all_data[cat].insert(0, ele)
            #如果添加后超过500，去掉后面几个
            diff_500 = len(all_data[cat]) - 500
            while diff_500 > 0:
                all_data[cat].pop(-1)
                diff_500 -= 1
        else:
            all_data[cat] = add_sql_data[cat]

    #hbase
    hbase_all_data = {}

    i = 0
    j = 0
    with open('resources/hbase_all.text', 'r') as f:
        for line in f:
            if line.startswith('ic_key'):
                ic_key_value = ic_key_re.search(line)
                path = ic_key_value.group()
                i += 1
            if line.startswith('doc_info'):
                doc_info_value = doc_info_re.findall(line)
                j += 1
            if i >= 1 and j == 1:
                hbase_all_data[path] = doc_info_value
                i = 0
                j = 0

    # diff
    diff_keyvalue = {}
    for key, value in all_data.items():
        if hbase_all_data.__contains__(key):
            len_sql = len(all_data[key])
            len_hbase = len(hbase_all_data[key])
            if len_sql != len_hbase:
                print(key+'length is different: sql->'+str(len_sql) +" hbase->"+str(len_hbase))
                continue
            # 逐个元素比较
            sql_hbase = zip(all_data[key], hbase_all_data[key])
            for ele in sql_hbase:
                if ele[0] != ele[1]:
                    diff_keyvalue[key] = ele
                    continue
        else:
            print('hbase miss path:' + key)

    if diff_keyvalue != {}:
        print('不同的二级类：')

        for key in diff_keyvalue.keys():
            print(key)
            print(diff_keyvalue[key])


def check_api(api_file_name):
    ic_key_re = re.compile('[0-9]{2}\..*\.[0-9]{2}')
    doc_info_re = re.compile('\d{3,8}')

    api ="http://st-try.dangdang.com/api/jcomment/newBook/bookShelf?timestamp=1635733912&client_version=11.10.0&" \
         "user_client=android&udid=c74e3d6c2f5cd10b13f6c1be9a6f098e&union_id=dangdang&" \
         "permanent_id=20211013095414725251649221857577845&" \
         "1635733913084=&page=1&pageSize=500&category={category}&type=shelf&ct=android&cv=11.10.0&ts=1635735750007&tc=bc761cecdb4ed0b5d9b2bb361955d087"

    #hbase data
    # hbase
    hbase_all_data = {}

    i = 0
    j = 0
    with open('resources/hbase_result.text', 'r') as f:
        for line in f:
            if line.startswith('ic_key'):
                ic_key_value = ic_key_re.search(line)
                path = ic_key_value.group()
                i += 1
            if line.startswith('doc_info'):
                doc_info_value = set(doc_info_re.findall(line))
                j += 1
            if i >= 1 and j == 1:
                hbase_all_data[path] = doc_info_value
                i = 0
                j = 0

    #选取两个分类进行测试
    #育儿/早教、时尚/美妆、工具书
    category_path =['01.17.00.00.00.00','01.11.00.00.00.00','01.50.00.00.00.00']
    for category in category_path:
        api_data = requests.get(api.format(category = category))
        api_json_data = json.loads(api_data.text)
        data = api_json_data['data']

        api_product_id_set = {ele['productId'] for ele in data}

        if hbase_all_data[category] ^ api_product_id_set  != set():

            diff_set = hbase_all_data[category] - api_product_id_set
            if diff_set != set():
                print({category:diff_set})

    pass




def reco_newbook_main():
    creation_date = '2021-11-11'
    filename = "resources/reco_newbook_full_{}".format(creation_date)
    #全量
    # new_book(filename,creation_date)

    #增量
    last_changed_date = '2021-09-15 15:00:00'
    cate_id_path_map_path = "resources/cat_id.text"
    new_book_add(cate_id_path_map_path,creation_date,last_changed_date)

    #接口校验
    # api_file_name = "resources/api.txt"
    # check_api(api_file_name)

