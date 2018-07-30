import numpy as np
import pandas as pd
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, mean_squared_log_error
from sklearn.preprocessing import StandardScaler, MinMaxScaler, MaxAbsScaler
from sklearn.model_selection import KFold
import matplotlib.pyplot as plt


data = pd.read_csv('../input/train.csv')
target = np.log1p(data['target'])
data.drop(['ID', 'target'], axis=1, inplace=True)

# add train leak
leak = pd.read_csv('../cache/train_leak.csv')
data['leak'] = leak['compiled_leak'].values
data['log_leak'] = np.log1p(leak['compiled_leak'].values)

# feature scoring usingxgboost with the leak feature
def rmse(y_true, y_pred):
    return mean_squared_error(y_true, y_pred) ** .5

reg = XGBRegressor(n_estimators=1000)
folds = KFold(4, True, 134259)
fold_idx = [(trn_, val_) for trn_, val_ in folds.split(data)]
scores = []

nb_values = data.nunique(dropna=False)
nb_zeros = (data == 0).astype(np.uint8).sum(axis=0)

features = [f for f in data.columns if f not in ['log_leak', 'leak', 'target', 'ID']]
for _f in features:
    score = 0
    for trn_, val_ in fold_idx:
        reg.fit(
            data[['log_leak', _f]].iloc[trn_], target.iloc[trn_],
            eval_set=[(data[['log_leak', _f]].iloc[val_], target.iloc[val_])],
            eval_metric='rmse',
            early_stopping_rounds=50,
            verbose=False
        )
        score += rmse(target.iloc[val_], reg.predict(data[['log_leak', _f]].iloc[val_], ntree_limit=reg.best_ntree_limit)) / folds.n_splits
    scores.append((_f, score))

# create  dataframe 获取了每一个特征与泄露特征进行同时训练时候得到的分数
report = pd.DataFrame(scores, columns=['feature', 'rmse']).set_index('feature')
# 新增了nb_zeros表示该行为0的数量，nunique表示表示该行是否为重复行
report['nb_zeros'] = nb_zeros
report['nunique'] = nb_values
# 根据分数进行正排
report.sort_values(by='rmse', ascending=True, inplace=True)
report.to_csv('../cache/feature_report.csv', index=True)

# scelet some features (threshold is not optimized) 选择rmse分数地域0.7952的值 的特征集
good_features = report.loc[report['rmse'] <= 0.7925].index
rmses = report.loc[report['rmse'] <= 0.7925, 'rmse'].values
good_features

# add test and test leak
test = pd.read_csv('../input/test.csv')
tst_leak = pd.read_csv('../cache/test_leak.csv')
test['leak'] = tst_leak['compiled_leak']
test['log_leak'] = np.log1p(tst_leak['compiled_leak'])

# train lightgbm
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import KFold
import lightgbm as lgb

folds = KFold(n_splits=5, shuffle=True, random_state=1)

# Use all features for stats
features = [f for f in data if f not in ['ID', 'leak', 'log_leak', 'target']]
# 将训练集中的0使用nan进行代替
data.replace(0, np.nan, inplace=True)
# 添加一些统计特征，基于行
data['log_of_mean'] = np.log1p(data[features].replace(0, np.nan).mean(axis=1))
data['mean_of_log'] = np.log1p(data[features]).replace(0, np.nan).mean(axis=1)
data['log_of_median'] = np.log1p(data[features].replace(0, np.nan).median(axis=1))
data['nb_nans'] = data[features].isnull().sum(axis=1)
data['the_sum'] = np.log1p(data[features].sum(axis=1))
data['the_std'] = data[features].std(axis=1)
data['the_kur'] = data[features].kurtosis(axis=1)

test.replace(0, np.nan, inplace=True)
test['log_of_mean'] = np.log1p(test[features].replace(0, np.nan).mean(axis=1))
test['mean_of_log'] = np.log1p(test[features]).replace(0, np.nan).mean(axis=1)
test['log_of_median'] = np.log1p(test[features].replace(0, np.nan).median(axis=1))
test['nb_nans'] = test[features].isnull().sum(axis=1)
test['the_sum'] = np.log1p(test[features].sum(axis=1))
test['the_std'] = test[features].std(axis=1)
test['the_kur'] = test[features].kurtosis(axis=1)

# Only use good features, log leak and stats for training 将最好的特征+统计特征
features = good_features.tolist()
features = features + ['log_leak', 'log_of_mean', 'mean_of_log', 'log_of_median', 'nb_nans', 'the_sum', 'the_std', 'the_kur']
dtrain = lgb.Dataset(data=data[features],
                     label=target, free_raw_data=False)
test['target'] = 0

dtrain.construct()
#创建空集合
oof_preds = np.zeros(data.shape[0])

for trn_idx, val_idx in folds.split(data):
    lgb_params = {
        'objective': 'regression',
        'num_leaves': 58,
        'subsample': 0.6143,
        'colsample_bytree': 0.6453,
        'min_split_gain': np.power(10, -2.5988),
        'reg_alpha': np.power(10, -2.2887),
        'reg_lambda': np.power(10, 1.7570),
        'min_child_weight': np.power(10, -0.1477),
        'verbose': -1,
        'seed': 3,
        'boosting_type': 'gbdt',
        'max_depth': -1,
        'learning_rate': 0.05,
        'metric': 'l2',
    }

    clf = lgb.train(
        params=lgb_params,
        train_set=dtrain.subset(trn_idx),
        valid_sets=dtrain.subset(val_idx),
        num_boost_round=10000,
        early_stopping_rounds=100,
        verbose_eval=0
    )

    # 为每一个验证集的下标指定已经模型的输出值
    oof_preds[val_idx] = clf.predict(dtrain.data.iloc[val_idx])
    # target取平均值
    test['target'] += clf.predict(test[features]) / folds.n_splits
    # 打印分值
    print(mean_squared_error(target.iloc[val_idx],
                             oof_preds[val_idx]) ** .5)
# 将训练集的predictions修改为已经推荐的结果。
data['predictions'] = oof_preds
# 将泄漏值不为null的值使用推荐值进行替代
data.loc[data['leak'].notnull(), 'predictions'] = np.log1p(data.loc[data['leak'].notnull(), 'leak'])
print('OOF SCORE : %9.6f'
      % (mean_squared_error(target, oof_preds) ** .5))
print('OOF SCORE with LEAK : %9.6f'
      % (mean_squared_error(target, data['predictions']) ** .5))



# save submission
test['target'] = np.expm1(test['target'])
test.loc[test['leak'].notnull(), 'target'] = test.loc[test['leak'].notnull(), 'leak']
test[['ID', 'target']].to_csv('../cache/leaky_submission.csv', index=False, float_format='%.2f')