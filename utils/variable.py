
#移动悟空筛选条件字典
source_dict = {"all": "全部", '1': "主站", '2': "天猫", '3': "抖音", '4': "拼多多"}
parent_platform_dict = {"all": "全部", '1': 'APP', '2': '轻应用', '3': 'H5', "4": 'PC'}
platform_dict = {"all": "全部", '1': "安卓", '2': "iOS", '3': "快应用", '4': "微信", '5': '百度', '6': '头条', '7': '支付宝', '8': 'qq',
            '9': '360'}

bd_id_dict = {"all": "全部", "1": "出版物", "2": "日百","3":"数字", "4": "文创",'5':"其他"}
shop_type = {"all": "全部", "1": "自营", "2": "招商"}
eliminate_type_dict = {"all": "不剔除", "1": "剔除建工"}
sale_type_dict = {"sd": "收订", "zf": "支付", "ck": "出库"}

app_dict={'1':'安卓','2':'IOS'}

customer_dict={'1':'新客','2':'老客'}
#bd_id=1时，下面平台
cat_name_dict={'1':'科技','2':'教育','3':'文艺','4':'生活','5':'社科','6':'经营','7':'音像','8':'原版','9':'童书','10':'others'}

zf_zhibiao=['出库金额','出库订单量', '出库包裹数',"出库单均价", "出库客单价",'出库实付金额','出库净销售额','毛利额','毛利率']

drill_common_dict = {'create_price': '金额', 'create_parent_amt': '订单量', 'create_order_amt': '包裹数',
               "priceByParent": "单均价", "priceByPerson": "客单价",'transRate':'转化率'}

drill_cancel_dict={"cancel_rate":"取消率"}
drill_zf_ck_dict={'out_pay_amount': '实付金额', 'out_profit': '净销售额'}
drill_ck = { 'gross_profit': '毛利额', 'gross_profit_rate': '毛利率'}