# -*- coding: utf-8 -*-
"""
Created on Sun May 13 10:33:05 2018
@author: Administrator
"""
import time
s_time = time.time()
import pandas as pd
#import numpy as np
#from datetime import datetime,timedelta
from util import feat_nunique,feat_sum,feat_count,feat_max,load_data,feat_mean,feat_min,feat_median
from lgb import get_result,get_train

order,action = load_data()
order['o_date'] = pd.to_datetime(order['o_date'])
action['a_date'] = pd.to_datetime(action['a_date'])

def create_feat(start_date,end_date,order,action,test=False):
    order = order.sort_values('o_date')
    action = action.sort_values('a_date')

    print start_date
    #构建用户特征集
    df_label = pd.read_csv(r'../data/jdata_user_basic_info.csv')

    #计算order和action与预测月份的时间差值
    enddate = pd.to_datetime(end_date)
    order.loc[:,'day_gap'] = order['o_date'].apply(lambda x:(x-enddate).days).copy()
    action.loc[:,'day_gap'] = action['a_date'].apply(lambda x:(x-enddate).days).copy()

    if test:
        df_label['label_1'] = -1
        df_label['label_2'] = -1
    else:
        #预测目标月份的数据
        label_month=pd.to_datetime(end_date).month
        #----------------------------------------------------------------------------------------------------------------------------------
        #找到用户在目标月份最早的订单日期
        order_label = order[order['o_month']==label_month]
        order_label = order_label[(order_label['cate']==30)|(order_label['cate']==101)]
        label = order_label.sort_values("o_date").drop_duplicates(["user_id"], keep="first")
        df_label = df_label.merge(label[['user_id','o_date']],on='user_id',how='left')

        #使用nunique和count方法计算的订单数不一致，说明订单表中的订单不是唯一标识符
        #lable_1代表用户在目标月份购买的订单个数，label_2代表用户在一个月的几号下的订单
        #label_2使用day-1比day的效果好说明系统趋向0的时候效果更好，这个需要之后再来矫正
        df_label = feat_nunique(df_label,order_label,['user_id'],'o_id','label_1')
        df_label['label_1']=[x if x<=3 else 3 for x in df_label['label_1']]
        df_label['label_2'] = [x.day if pd.to_datetime(x)>=pd.to_datetime(start_date) else 0 for x in df_label['o_date']]
        del df_label['o_date']

    #用户总体特征
    order_tmp = order[order['day_gap']<0]
    action_tmp = action[action['day_gap']<0]

    #放入两个同样的特征，虽然新特征的分数较高，但是对整体无帮助
    #order_tmp_dup = order_tmp.sort_values('o_month').drop_duplicates(["user_id"],keep='last')
    #action_tmp_dup = action_tmp.sort_values('o_date').drop_duplicates(["user_id"],keep='first')

    df_label = feat_nunique(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'sku_id','sku_id_30_101_nunique')
    df_label=feat_sum(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'price','price_sum')
    df_label=feat_mean(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'price','price_mean')
    df_label=feat_min(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'price','price_min')
    df_label=feat_max(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'price','price_max')
    df_label=feat_median(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'price','price_median')

    comment=order_tmp[order_tmp['score_level']>0]
    comment_1 = comment[comment['score_level']==1]
    comment_2 = comment[comment['score_level']==2]
    comment_3 = comment[comment['score_level']==3]
    df_label=feat_mean(df_label,comment[(comment['cate']==30)|(comment['cate']==101)],['user_id'],'score_level','user_comment_score_mean')
    df_label=feat_sum(df_label,comment_1[(comment_1['cate']==30)|(comment_1['cate']==101)],['user_id'],'score_level','user_comment_score_sum1')
    df_label=feat_sum(df_label,comment_2[(comment_2['cate']==30)|(comment_2['cate']==101)],['user_id'],'score_level','user_comment_score_sum2')
    df_label=feat_sum(df_label,comment_3[(comment_3['cate']==30)|(comment_3['cate']==101)],['user_id'],'score_level','user_comment_score_sum3')

    df_label = feat_nunique(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'o_id','o_id_30_101_nunique')
    df_label = feat_count(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'sku_id','o_sku_id_30_101_count')
    df_label = feat_sum(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'o_sku_num','o_sku_num_30_101_sum')
    df_label = feat_mean(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'o_day','day_30_101_mean')
    df_label = feat_nunique(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'o_date','o_date_30_101_nunique')
    df_label = feat_nunique(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'o_month','o_month_30_101_nunique')
    df_label = feat_count(df_label,action_tmp[(action_tmp['cate']==30)|(action_tmp['cate']==101)],['user_id'],'sku_id','a_sku_id_30_101_count')
    df_label = feat_nunique(df_label,action_tmp[(action_tmp['cate']==30)|(action_tmp['cate']==101)],['user_id'],'a_date','a_date_30_101_nunique')

    #测试特征
    #从353降到3528,虽然user_lv_cd为1与其他的类别差距明显，但是其数量较少，偶然因素较大
    #df_label['user_lv_cd_1'] = [1 if x>1 else 0 for x in df_label['user_lv_cd']]



    for i in [7,14,30,90,180]:
        print i
        order_tmp = order[(order['day_gap']>=-i)&(order['day_gap']<0)]
        action_tmp = action[(action['day_gap']>=-i)&(action['day_gap']<0)]

        a = "AD"+str(i)+"_"
        o = "OD"+str(i)+"_"

        df_label=feat_sum(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'price',o+'price_sum')
        df_label=feat_mean(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'price',o+'price_mean')
        df_label=feat_min(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'price',o+'price_min')
        df_label=feat_max(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'price',o+'price_max')
        df_label=feat_median(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'price',o+'price_median')

        #######################################################################################################################################
        #用户当月(30,101),30,101,(!30 101)多少个订单
        df_label = feat_nunique(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'o_id',o+'o_id_30_101_nunique')
        df_label = feat_nunique(df_label,order_tmp[order_tmp['cate']==30],['user_id'],'o_id',o+'o_id_30_nunique')
        df_label = feat_nunique(df_label,order_tmp[order_tmp['cate']==101],['user_id'],'o_id',o+'o_id_101_nunique')
        df_label = feat_nunique(df_label,order_tmp[(order_tmp['cate']!=30)&(order_tmp['cate']!=101)],['user_id'],'o_id',o+'o_id_other_nunique')

        #######################################################################################################################################
        #用户当月(30,101),30,101,(!30 101)买了多少次商品
        df_label = feat_count(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'sku_id',o+'sku_id_30_101_count')
        df_label = feat_count(df_label,order_tmp[order_tmp['cate']==30],['user_id'],'sku_id',o+'sku_id_30_count')
        df_label = feat_count(df_label,order_tmp[order_tmp['cate']==101],['user_id'],'sku_id',o+'sku_id_101_count')
        df_label = feat_count(df_label,order_tmp[(order_tmp['cate']!=30)&(order_tmp['cate']!=101)],['user_id'],'sku_id',o+'sku_id_other_count')

        #######################################################################################################################################
        #用户当月(30,101),30,101,(!30 101)买的商品的数量
        df_label = feat_sum(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'o_sku_num',o+'o_sku_num_30_101_count')
        df_label = feat_sum(df_label,order_tmp[order_tmp['cate']==30],['user_id'],'o_sku_num',o+'o_sku_num_30_count')
        df_label = feat_sum(df_label,order_tmp[order_tmp['cate']==101],['user_id'],'o_sku_num',o+'o_sku_num_101_count')
        df_label = feat_sum(df_label,order_tmp[(order_tmp['cate']!=30)&(order_tmp['cate']!=101)],['user_id'],'o_sku_num',o+'o_sku_num_other_count')

        ########################################################################################################################################
        #用户当月(30,101),30,101首次订单产生是哪一天,这个特征不太合理
        df_label[o+'o_date_30_101_firstday'] = order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)].drop_duplicates(['user_id'],keep='first')['o_day']
        #df_label[o+'o_date_30_101_lastday'] = order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)].drop_duplicates(['user_id'],keep='last')

        #######################################################################################################################################
        #用户当月(30,101),30,101最后一次订单day,对分数有提升，说明这个特征是有用的
        df_label = feat_max(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'o_day',o+'day_30_101_max')
        df_label = feat_max(df_label,order_tmp[order_tmp['cate']==30],['user_id'],'o_day',o+'day_30_max')
        df_label = feat_max(df_label,order_tmp[order_tmp['cate']==101],['user_id'],'o_day',o+'day_101_max')

        ####################################################################################################################################
        #用户当月(30,101),30,101平均第几天购买
        df_label = feat_mean(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'o_day',o+'day_30_101_mean')
        df_label = feat_mean(df_label,order_tmp[order_tmp['cate']==30],['user_id'],'o_day',o+'day_30_mean')
        df_label = feat_mean(df_label,order_tmp[order_tmp['cate']==101],['user_id'],'o_day',o+'day_101_mean')

        ####################################################################################################################################
        #用户当月(30,101),30,101购买天数
        df_label = feat_nunique(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'o_date',o+'o_date_30_101_mean')
        df_label = feat_nunique(df_label,order_tmp[order_tmp['cate']==30],['user_id'],'o_date',o+'o_date_30_mean')
        df_label = feat_nunique(df_label,order_tmp[order_tmp['cate']==101],['user_id'],'o_date',o+'o_date_101_mean')
        ####################################################################################################################################
        #用户当月(30,101),30,101购买月份数
        df_label = feat_nunique(df_label,order_tmp[(order_tmp['cate']==30)|(order_tmp['cate']==101)],['user_id'],'o_month',o+'o_month_30_101_nunique')
        df_label = feat_nunique(df_label,order_tmp[order_tmp['cate']==30],['user_id'],'o_month',o+'o_month_30_nunique')
        df_label = feat_nunique(df_label,order_tmp[order_tmp['cate']==101],['user_id'],'o_month',o+'o_month_101_nunique')


        ####################################################################################################################################
        #用户在当月份的两种行为
        action_tmp_type_1 = action_tmp[action_tmp['a_type']==1]
        action_tmp_type_2 = action_tmp[action_tmp['a_type']==2]

        ####################################################################################################################################
        #用户当月(30,101),30,101行为次数
        df_label = feat_count(df_label,action_tmp[(action_tmp['cate']==30)|(action_tmp['cate']==101)],['user_id'],'sku_id',a+'sku_id_30_101_count')
        df_label = feat_count(df_label,action_tmp[action_tmp['cate']==30],['user_id'],'sku_id',a+'sku_id_30_count')
        df_label = feat_count(df_label,action_tmp[action_tmp['cate']==101],['user_id'],'sku_id',a+'sku_id_101_count')

        ####################################################################################################################################
        #用户当月(30,101),30,101浏览行为次数
        df_label = feat_count(df_label,action_tmp_type_1[(action_tmp_type_1['cate']==30)|(action_tmp_type_1['cate']==101)],['user_id'],'sku_id',a+'sku_id_type1_30_101_count')
        df_label = feat_count(df_label,action_tmp_type_1[action_tmp_type_1['cate']==30],['user_id'],'sku_id',a+'sku_id_type1_30_count')
        df_label = feat_count(df_label,action_tmp_type_1[action_tmp_type_1['cate']==101],['user_id'],'sku_id',a+'sku_id_type1_101_count')

        ####################################################################################################################################
        #用户当月(30,101),30,101收藏行为次数
        df_label = feat_count(df_label,action_tmp_type_2[(action_tmp_type_2['cate']==30)|(action_tmp_type_2['cate']==101)],['user_id'],'sku_id',a+'sku_id_type2_30_101_count')
        df_label = feat_count(df_label,action_tmp_type_2[action_tmp_type_2['cate']==30],['user_id'],'sku_id',a+'sku_id_type2_30_count')
        df_label = feat_count(df_label,action_tmp_type_2[action_tmp_type_2['cate']==101],['user_id'],'sku_id',a+'sku_id_type2_101_count')

        ####################################################################################################################################
        #用户当月(30,101),30,101行为天数
        df_label = feat_nunique(df_label,action_tmp[(action_tmp['cate']==30)|(action_tmp['cate']==101)],['user_id'],'a_date',a+'a_date_30_101_nunique')
        df_label = feat_nunique(df_label,action_tmp[action_tmp['cate']==30],['user_id'],'a_date',a+'a_date_30_nunique')
        df_label = feat_nunique(df_label,action_tmp[action_tmp['cate']==101],['user_id'],'a_date',a+'a_date_101_nunique')

        ####################################################################################################################################
        #用户当月(30,101),30,101行为商品数
        df_label = feat_nunique(df_label,action_tmp[(action_tmp['cate']==30)|(action_tmp['cate']==101)],['user_id'],'sku_id',a+'sku_id_30_101_nunique')
        df_label = feat_nunique(df_label,action_tmp[action_tmp['cate']==30],['user_id'],'sku_id',a+'sku_id_30_nunique')
        df_label = feat_nunique(df_label,action_tmp[action_tmp['cate']==101],['user_id'],'sku_id',a+'sku_id_101_nunique')

    print df_label.shape
    return df_label

def gen_vali(month_8=False):
    train = create_feat('2017-5-1','2017-6-1',order,action)
    train = pd.concat([train,create_feat('2017-4-1','2017-5-1',order,action)])
    train = pd.concat([train,create_feat('2017-3-1','2017-4-1',order,action)])
    train = pd.concat([train,create_feat('2017-2-1','2017-3-1',order,action)])
    train = pd.concat([train,create_feat('2017-1-1','2017-2-1',order,action)])

    train.to_csv(r'../input/vali_train.csv',index=None)

    test = create_feat('2017-6-1','2017-7-1',order,action)
    test.to_csv(r'../input/vali_test.csv',index=None)

    if month_8:
        train = pd.concat([train,test])
        train.to_csv(r'../input/vali2_train.csv',index=None)
        test = create_feat('2017-7-1','2017-8-1',order,action)
        test.to_csv(r'../input/vali2_test.csv',index=None)

def gen_test():

    train = create_feat('2017-7-1','2017-8-1',order,action)
    train = pd.concat([train,create_feat('2017-6-1','2017-7-1',order,action)])
    train = pd.concat([train,create_feat('2017-5-1','2017-6-1',order,action)])
    train = pd.concat([train,create_feat('2017-4-1','2017-5-1',order,action)])
    train = pd.concat([train,create_feat('2017-3-1','2017-4-1',order,action)])
    train = pd.concat([train,create_feat('2017-2-1','2017-3-1',order,action)])

    train.to_csv(r'../input/test_train.csv',index=None)

    test = create_feat('2017-8-1','2017-9-1',order,action,True)
    test.to_csv(r'../input/test_test.csv',index=None)

if __name__ == '__main__':
    gen_test()
    s1,s2 = get_result()

    #gen_vali(True)
    #feat_imp_s1,feat_imp_s2 = get_train()

    print "feature exacting tasks %d second"%(time.time()-s_time)