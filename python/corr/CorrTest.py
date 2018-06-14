from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
spark=SparkSession.builder.appName('corrs').getOrCreate()
df=spark.read.parquet(basePath+"corr/label1_corr").toDF('column','corr').orderBy('corr')

df.show()
pd_df=df.select("corr").toPandas()
print(pd_df)
index=df.select('column').toPandas().values

# pd_df=pd.DataFrame(np.random.rand(5,1),index=['one','two','four','five','six'],columns=['first'])
# print(pd_df)
# index=      pd_df['first'].values.tolist()
# index2=['1','2','3','4','5']
# print(index)
#
# print(type(index))
# pd_df=pd_df.reindex(index)
# print(pd_df)
pd_df.plot(kind='bar')
plt.show()