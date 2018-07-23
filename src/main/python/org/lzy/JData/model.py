# -*- coding: utf-8 -*-
"""
Created on Fri May 25 14:57:21 2018
@author: Administrator
"""

import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import train_test_split
#from sklearn.model_selection import KFold
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import mean_squared_error

param = {
    'task': 'train',
    'boosting_type': 'gbdt',
    'objective': 'regression',
    'metric': {'l2'},
    'num_leaves': 31,
    'learning_rate': 0.05,
    'feature_fraction': 0.9,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'verbose': 0
}

def fit_predict(X,y,X_pred):
    predictors = [i for i in X.columns]
    stacking_num = 5
    bagging_num = 3
    bagging_test_size = 0.33
    num_boost_round = 500
    early_stopping_rounds = 100
                
    stacking_model=[]
    bagging_model=[]

    l2_error = []
    X = X.values
    y=y.values
    layer_train = np.zeros((X.shape[0],2))
    SK = StratifiedKFold(n_splits=stacking_num, shuffle=True, random_state=1)
    for k,(train_index,test_index) in enumerate(SK.split(X,y)):
        X_train = X[train_index]
        y_train = y[train_index]
        X_test = X[test_index]
        y_test = y[test_index]
        
        lgb_train = lgb.Dataset(X_train,y_train)
        lgb_eval = lgb.Dataset(X_test,y_test)
        
        gbm=lgb.train(param,lgb_train,num_boost_round=num_boost_round,valid_sets=lgb_eval,early_stopping_rounds=early_stopping_rounds)
        stacking_model.append(gbm)
        
    X = np.hstack((X,layer_train[:,1].reshape((-1,1))))
    
    predictors.append('lgb_result')
    
    for bn in range(bagging_num):
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=bagging_test_size, random_state=bn)
        
        lgb_train = lgb.Dataset(X_train,y_train)
        lgb_eval = lgb.Dataset(X_test,y_test)
        
        gbm = lgb.train(param,lgb_train,num_boost_round=10000,valid_sets=lgb_eval,early_stopping_rounds=200)
        
        bagging_model.append(gbm)
        
        l2_error.append(mean_squared_error(gbm.predict(X_test,num_iteration=gbm.best_iteration),y_test))
        
        feat_imp = pd.Series(gbm.feature_importance(), predictors).sort_values(ascending=False)
        
    test_pred = np.zeros((X_pred.shape[0],stacking_num))
    for sn,gbm in enumerate(stacking_model):
        pred = gbm.predict(X_pred,num_iteration=gbm.best_iteration)
        test_pred[:,sn] = pred
        
        X_pred = np.hstack((X_pred,test_pred.mean(axis=1).reshape((-1,1))))
        
    for bn,gbm in enumerate(bagging_model):
        pred = gbm.predict(X_pred,num_iteration=gbm.best_iteration)
        if bn==0:
            pred_out = pred
        else:
            pred_out += pred
    return pred_out/bagging_num,feat_imp