import numpy as np
import pandas as pd

basePath='E:\\dataset\\JData_UserShop\\'

order = pd.read_csv(basePath+'B/jdata_user_order.csv')
sku = pd.read_csv(basePath+'B/jdata_sku_basic_info.csv')
print(order.head())
#
# print(order['o_date'].drop_duplicates().size)
# order_sku=order.merge(sku,on='sku_id',how='left')
#
# date2count=pd.DataFrame(order_sku[order_sku['cate'] == 30].groupby('o_date')['user_id'].count()).reset_index()
# # date2count.columns=['o_date','count']
# print(date2count)
#
# print(date2count[pd.to_datetime(date2count['o_date']).dt.month == 5])
# values=date2count[pd.to_datetime(date2count['o_date']).dt.month == 5]['user_id'].values
# print(sum(values))

