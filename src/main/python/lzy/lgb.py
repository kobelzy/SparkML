# -*- coding: utf-8 -*-
"""
Created on Tue May 01 15:16:59 2018
@author: Administrator
"""
import pandas as pd
import numpy as np
from model import fit_predict
from datetime import datetime,timedelta
import time
s_time = time.time()

weight = []
for i in range(1,50001):
    weight.append(1/(1+np.log(i)))
    
def score(df):
    sub = df.sort_values('o_num',ascending=False).reset_index(drop='index')
    sub['label_1'] = [1 if x>0 else 0 for x in sub['label_1']]
    sub = sub.loc[:49999,:]
    sub['weight'] = weight
    
    s1 = sum(sub['label_1']*sub['weight'])/4674.323
    
    s2=0.0
    df =df[df['label_1']>0].reset_index(drop='index')
    for i in range(sub.shape[0]):
        if sub.loc[i,'label_1']>0:
            s2 += 10.0/((sub.loc[i,'label_2']-np.round(sub.loc[i,'pred_date']))**2+10)
    
    s2=s2/df.shape[0]
        
    return s1,s2

def get_train():
    #读取数据
    train = pd.read_csv(r'../input/vali_train.csv')
    test = pd.read_csv(r'../input/vali_test.csv')
    train['label_1'] = [1 if x>0 else 0 for x in train['label_1']]
    test['label_1'] = [1 if x>0 else 0 for x in test['label_1']]
    drop_column = ['user_id','label_1','label_2']
    result = test[['user_id']]#要提交的结果
    #训练S1
    X = train.drop(drop_column,axis=1)
    X_pred = test.drop(drop_column,axis=1)
    y = train['label_1']
    result['o_num'],feat_imp_s1 = fit_predict(X,y,X_pred)
    #训练S2
    y = train['label_2']
    result['pred_date'], feat_imp_s2= fit_predict(X,y,X_pred)
    #输出结果
    result[['label_1','label_2']] = test[['label_1','label_2']]
    s1,s2=score(result)
    
    train = pd.read_csv(r'../input/vali2_train.csv')
    test = pd.read_csv(r'../input/vali2_test.csv')
    train['label_1'] = [1 if x>0 else 0 for x in train['label_1']]
    test['label_1'] = [1 if x>0 else 0 for x in test['label_1']]
    result = test[['user_id']]#要提交的结果
    #训练S1
    X = train.drop(drop_column,axis=1)
    X_pred = test.drop(drop_column,axis=1)
    y = train['label_1']
    result['o_num'],feat_imp_s1 = fit_predict(X,y,X_pred)
    #训练S2
    y = train['label_2']
    result['pred_date'], feat_imp_s2= fit_predict(X,y,X_pred)
    #输出结果
    result[['label_1','label_2']] = test[['label_1','label_2']]
    s3,s4=score(result)
    
    print 'month 3: s1 %s    s2 %s   S:%s'%(s1,s2,0.4*s1+0.6*s2)
    print 'month 4: s1 %s    s2 %s   S:%s'%(s3,s4,0.4*s3+0.6*s4)
    return feat_imp_s1,feat_imp_s2

def get_result():
    train = pd.read_csv(r'../input/test_train.csv')
    test = pd.read_csv(r'../input/test_test.csv')
    drop_column = ['user_id','label_1','label_2']
    
    sub = test[['user_id']]#要提交的结果
    
    #训练S1
    X = train.drop(drop_column,axis=1)
    X_pred = test.drop(drop_column,axis=1)
    y = train['label_1']
    
    sub['o_num'],s1 = fit_predict(X,y,X_pred)
    
    X = train.drop(drop_column,axis=1)
    X_pred = test.drop(drop_column,axis=1)
    y=train['label_2']
    sub['pred_date'],s2 = fit_predict(X,y,X_pred)
    
    #输出结果
    sub = sub.sort_values(by='o_num',ascending=False)[['user_id','pred_date']].reset_index(drop='index')
    sub['pred_date'] = sub['pred_date'].map(lambda day: datetime(2017, 5, 1)+timedelta(days=np.round(day-1)))
    sub.loc[:49999,:].to_csv(r'../result/submission.csv',index=None,encoding='utf-8')
    
    return s1,s2


if __name__ =='__main__':
    #加载训练集和测试集
    #feat_imp_s1,feat_imp_s2 = get_train()
    s1,s2 = get_result()
    
    print "feature exacting tasks %d second"%(time.time()-s_time)