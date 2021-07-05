#推荐榜单
'''
图书畅销-24小时  		 `bang_bestsell_bk_24hours`
图书畅销-24小时-童书榜   `bang_bestsell_bk_24hours_ts`
新书热卖-24小时  		 `bang_newhot_bk_24hours
新书热卖-24小时下手动过滤童书分类：01.41.00.00.00.00  bang_newhot_bk_24hours_ts

'''
import re
import happybase
from utils.db import connect_mysql_from_jump_server

table={
    '图书畅销榜':'bang_bestsell_bk_24hours',
    '新书热卖榜': 'bang_newhot_bk_24hours',
    '童书畅销榜':'bang_bestsell_bk_24hours_ts',
    '童书新书榜':'bang_newhot_bk_24hours_ts'
}




def bangfile_bangsql(bang_info_list,datadict):

    for each_line in bang_info_list:
        line = each_line.split('\t')

        pid = line[0]

        bangurl = line[1]
        bang_name_text_sec_path = re.findall('bang_name_text=(.*?)&(?:.*)sec_catpath=(.*?)&', bangurl)

        #优先级 图书畅销榜＞新书热卖榜＞童书畅销榜＞童书新书榜

        bang_name,sec_path = bang_name_text_sec_path[0]

        for bang in table.keys():

            if sec_path in datadict[bang].keys():

                if pid in datadict[bang][sec_path]:

                    if bang_name != bang :
                        #优先级或榜单名称错误
                        print(str(pid)+' now in '+bang_name+' but shoud in '+bang)
                    else:
                        break

#hbase 校验
def bangfile_to_hbase():
    file = 'part-0000.txt'

    conn = happybase.Connection(host='10.5.24.53', port=9090,timeout=10000)
    # visit habse
    table = happybase.Table('ddse_product_clonefromsnapshot', conn)

    with open('./loadfile/'+file,'r',encoding='UTF-8') as bang_line_info:

        for each_line in bang_line_info:
            lineinfo = each_line.split('\t')

            rowkey = lineinfo[0]

            # if not rowkey[-3:].startswith('0'):  # hue 查询
            if True:
                rank_url = lineinfo[1]
                rank_id = lineinfo[2]
                rank_name = lineinfo[-1].strip('\n')

                hbasekey = rowkey[-3:]+rowkey
                data = table.row(hbasekey)     #字典(内容字节形式）

                # temp_data={}
                # for key,value in data.items():
                #     temp_data[key.decode()] = value.decode()

                hbase_rank_name = data[b'a:rank_name'].decode()
                hbase_rank_id = data[b'a:rank_id'].decode()
                hbase_rank_url = data[b'a:rank_url'].decode()


                print('now check '+ rowkey)
                if not (hbase_rank_name == rank_name and hbase_rank_id == rank_id and hbase_rank_url == rank_url) :
                    print('hbase and bangfile is different: '+ rowkey)




#推荐榜单测试
def test_reco_bang():

    file = 'part-0000.txt'

    sql = '''
         SELECT
            `category3_path`,
            '{table_name}' AS table_name,
             substring_index(GROUP_CONCAT(
                `product_id`
                ORDER BY
                    `sale_qty_rank` DESC),',',30
            ) AS ids
        FROM
            `{table_name}`
    '''
    where = ''
    groupby = " group by category3_path "


    #连接mysql数据库
    MYSQL_HOST = 'mynbdpbangdbr.idc3'
    MYSQL_PORT = 3306
    MYSQL_USER = 'reader'
    MYSQL_PASSWD = 'n0p4ssw0rd'
    MYSQL_DATABASE = 'nbdp_bang'

    server,cursor = connect_mysql_from_jump_server(MYSQL_HOST,MYSQL_PORT,MYSQL_USER,MYSQL_PASSWD,MYSQL_DATABASE)

    data_dict={}

    for table_cname,table_ename in table.items():

        if table_ename == 'bang_newhot_bk_24hours_ts':
            table_ename = 'bang_newhot_bk_24hours'
            where = " where category2_path ='01.41.00.00.00.00'"

        temp_sql = sql.format(table_name=table_ename) +where+groupby

        cursor.execute(temp_sql)

        rawdata = cursor.fetchall()

        data_dict[table_cname] = {}

        for item in rawdata:

            category = item[0]

            product_id_set = set(item[-1].split(','))

            data_dict[table_cname].update({category:product_id_set})



    with open('./loadfile/'+file,'r',encoding='UTF-8') as bang_line_info:
        bangfile_bangsql(bang_line_info,data_dict)
