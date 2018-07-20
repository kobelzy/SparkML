import pandas as pd

train =pd.read_csv("E:\\dataset\\Kaggle_Santander\\AData\\train.csv")
def combine_data(x):
    return " ".join(map(str,x))

# print(train.iloc[:,2:].apply(lambda x: combine_data(x),axis=0))
#
# print(train.shape)

print(train)

cols_with_onlyone_val = train.columns[train.nunique() == 1]
for ele in cols_with_onlyone_val:
    print(ele)