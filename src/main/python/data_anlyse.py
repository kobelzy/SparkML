import numpy as np
import pandas as pd

basePath='E:\\dataset\\JData_UserShop\\'

order = pd.read_csv(basePath+'B/jdata_user_order.csv')
sku = pd.read_csv(basePath+'B/jdata_sku_basic_info.csv')
print(order.head())

print(order['o_date'].drop_duplicates().size)


