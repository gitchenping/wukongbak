'''
mysql_table={
    'crm_supply_cat_month':'月度品类销售分布',
    'crm_supply_zhengti_xiaoshou':'历史销售&回款',
    'crm_supply_mayang_yunying_month':'月毛利&存货',
    'crm_supply_caigou_month':'上月采购码洋到货率',
    'crm_supply_huigao_month':'上月回告码洋',

    'crm_supply_daohuoshichang_month':'月平均到货时长',
    'crm_supply_product_month_top':'月销售TOP40',
    'crm_supply_product_year_top':'年销售TOP40',
    'crm_supply_warehouse_month_top':'天津仓售断TOP全品'
}
'''
#品 字段信息
product_dict={
    'supplier_num':'供应商编码',
    'supplier_name' :'供应商名称',
    'isbn': 'isbn',
    'product_id':'商品id',
    'product_name':'商品名称',
    'category_path2' :'二级类路径',
    'path2_name': '二级类名称',
    'original_price':'定价',
    'prod_sale_qty': '销售数量',
    'prod_sale_fixed_amt':'销售码洋',
    'num' : '排行'
}

warehouse_dict={
    'supplier_num': '供应商编码',
    'supplier_name': '供应商名称',
    'warehouse_id':'仓店id',
    'warehouse_name':'仓店名称',
    'isbn': 'isbn',
    'product_id': '商品id',
    'product_name': '商品名称',
    'category_path2': '二级类路径',
    'path2_name': '二级类名称',
    'original_price': '定价',
    'prod_sale_qty': '销售数量',
    'prod_sale_fixed_amt': '销售码洋',
    'num': '排行'
}
mayang_yunying_dict={
        'supplier_num':'供应商编码',
        'supplier_name':'供应商名称',
        'prod_sale_fixed_amt': '销售码洋',
        'return_rate': '退货率',
        'zhouzhuan_days': '周转天数',
       'sku_rate': '动销率',
        'amao_rate': '毛利率A',
        'qimo_sku_num': '期末在库品种数',
         'PROD_STOCK_COST_AMOUNT': '期末可销售库存成本',
         'duanhuo_rate': '全店售断率'
}