import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

train=pd.read_csv('E:\\dataset\\Kaggle_Santander\\AData\\train.csv')
train.head()

fig,axs=plt.subplots(1,1)
target=train[train['target'] >0]['target']
target=np.log1p(target)
# .hist(bins=10)
sns.distplot(target,hist=True,kde=True)
plt.show()