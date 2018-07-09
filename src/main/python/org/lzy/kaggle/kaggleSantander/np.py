import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# data=0
# test=np.tanh(np.where(data>0, ((data + (np.minimum(((-1.0)), ((2.0)))))/2.0), ((((5.0)) > (data))*1.) ))
# print(test)
# print(np.minimum(((-1.0)), ((2.0))))
#
# print(np.where(data>0,2,-2))
# print((5.0<1.0)*1.0)



# df=pd.DataFrame(np.arange(16).reshape(4,4))
# print(df)
# x1=df.sum(axis=1).values*.1
# print(x1)
# x1_reshape=x1.reshape(-1,1)
# print(x1_reshape)
# x2=np.arange(4).reshape(-1,1)
# print(x2)
# x3=np.arange(0,8,2).reshape(-1,1)
# print(x3)
# hstack=np.hstack([x1.reshape(-1,1),x2,x3])
# print(hstack)
# print(type(hstack))
#
#
# fig,axes=plt.subplots(1,1,figsize=(15,15))
# cm=plt.cm.get_cmap('RdYlBu')
# sc=axes.scatter(hstack[:,0],hstack[:,1],c=(hstack[:,2]),cmap=cm,s=30)
# cbar = fig.colorbar(sc, ax=axes)
# plt.show()

# arr=[[1,2,3],[1,1,1][1,2,1]]
df=pd.DataFrame(np.arange(16).reshape(4,4))
# df=pd.DataFrame(arr)
df[3]=df[3]-1
df[1]=2
print(df)

df_unique=np.unique(df,axis=1,return_index=True)
print(df_unique)


weight = ((df != 0).sum()/len(df))
print(weight)
print(type(weight))
print(type(weight.values))
# print((df!=0).sum())
