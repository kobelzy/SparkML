import numpy as np
import pandas as pd
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, mean_squared_log_error
from sklearn.preprocessing import StandardScaler, MinMaxScaler, MaxAbsScaler
from sklearn.model_selection import KFold
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import KFold
import lightgbm as lgb
from skopt import BayesSearchCV
from sklearn.model_selection import StratifiedKFold, KFold

data = pd.read_csv('../input/train.csv')
target = np.log1p(data['target'])
data.drop(['ID', 'target'], axis=1, inplace=True)

leak = pd.read_csv('../cache/train_leak.csv')
data['leak'] = leak['compiled_leak'].values
data['log_leak'] = np.log1p(leak['compiled_leak'].values)

# def rmse(y_true, y_pred):
#     return mean_squared_error(y_true, y_pred) ** .5
#
# reg = XGBRegressor(n_estimators=1000)
# folds = KFold(4, True, 134259)
# fold_idx = [(trn_, val_) for trn_, val_ in folds.split(data)]
# scores = []
#
# nb_values = data.nunique(dropna=False)
# nb_zeros = (data == 0).astype(np.uint8).sum(axis=0)
#
# features = [f for f in data.columns if f not in ['log_leak', 'leak', 'target', 'ID']]
# for _f in features:
#     score = 0
#     for trn_, val_ in fold_idx:
#         reg.fit(
#             data[['log_leak', _f]].iloc[trn_], target.iloc[trn_],
#             eval_set=[(data[['log_leak', _f]].iloc[val_], target.iloc[val_])],
#             eval_metric='rmse',
#             early_stopping_rounds=50,
#             verbose=False
#         )
#         score += rmse(target.iloc[val_], reg.predict(data[['log_leak', _f]].iloc[val_], ntree_limit=reg.best_ntree_limit)) / folds.n_splits
#     scores.append((_f, score))
#
# report = pd.DataFrame(scores, columns=['feature', 'rmse']).set_index('feature')
# report['nb_zeros'] = nb_zeros
# report['nunique'] = nb_values
# report.sort_values(by='rmse', ascending=True, inplace=True)
#
# report.to_csv('../cache/feature_report2.csv', index=True)

report = pd.read_csv('../cache/feature_report2.csv', index_col='feature')


good_features = report.loc[report['rmse'] <= 0.7955].index
rmses = report.loc[report['rmse'] <= 0.7955, 'rmse'].values

test = pd.read_csv('../input/test.csv')

tst_leak = pd.read_csv('../cache/test_leak.csv')
test['leak'] = tst_leak['compiled_leak']
test['log_leak'] = np.log1p(tst_leak['compiled_leak'])


folds = KFold(n_splits=5, shuffle=True, random_state=1)

# Use all features for stats
features = [f for f in data if f not in ['ID', 'leak', 'log_leak', 'target']]
data.replace(0, np.nan, inplace=True)
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


# Only use good features, log leak and stats for training
features = good_features.tolist()
features = features + ['log_leak', 'log_of_mean', 'mean_of_log', 'log_of_median', 'nb_nans', 'the_sum', 'the_std', 'the_kur']
dtrain = lgb.Dataset(data=data[features],
                     label=target, free_raw_data=False)
test['target'] = 0

dtrain.construct()
oof_preds = np.zeros(data.shape[0])





def status_print(optim_result):
    """Status callback durring bayesian hyperparameter search"""

    # Get all the models tested so far in DataFrame format
    all_models = pd.DataFrame(bayes_cv_tuner.cv_results_)

    # Get current parameters and the best parameters
    best_params = pd.Series(bayes_cv_tuner.best_params_)
    print('Model #{}\nBest MSE: {}\nBest params: {}\n'.format(
        len(all_models),
        np.round(bayes_cv_tuner.best_score_, 4),
        bayes_cv_tuner.best_params_
    ))

    # Save all model results
    clf_name = bayes_cv_tuner.estimator.__class__.__name__
    all_models.to_csv(clf_name+"_cv_results.csv")


import math

#A function to calculate Root Mean Squared Logarithmic Error (RMSLE)
def rmsle(y, y_pred):
    assert len(y) == len(y_pred)
    terms_to_sum = [(math.log(y_pred[i] + 1) - math.log(y[i] + 1)) ** 2.0 for i,pred in enumerate(y_pred)]
    return (sum(terms_to_sum) * (1.0/len(y))) ** 0.5




bayes_cv_tuner = BayesSearchCV(
    estimator = lgb.LGBMRegressor(objective='regression', boosting_type='gbdt', subsample=0.6143), #colsample_bytree=0.6453, subsample=0.6143
    search_spaces = {
        'learning_rate': (0.01, 1.0, 'log-uniform'),
        'num_leaves': (10, 100),
        'max_depth': (0, 50),
        'min_child_samples': (0, 50),
        'max_bin': (100, 1000),
        'subsample_freq': (0, 10),
        'min_child_weight': (0, 10),
        'reg_lambda': (1e-9, 1000, 'log-uniform'),
        'reg_alpha': (1e-9, 1.0, 'log-uniform'),
        'scale_pos_weight': (1e-6, 500, 'log-uniform'),
        'n_estimators': (50, 150),
    },
    scoring = 'neg_mean_squared_log_error', #neg_mean_squared_log_error
    cv = KFold(
        n_splits=5,
        shuffle=True,
        random_state=42
    ),
    n_jobs = 1,
    n_iter = 100,
    verbose = 0,
    refit = True,
    random_state = 42
)

# Fit the model
result = bayes_cv_tuner.fit(data[features], target, callback=status_print)

pred = bayes_cv_tuner.predict(test[features])

test['target'] = np.expm1(pred)
test[['ID', 'target']].to_csv('my_submission.csv', index=False, float_format='%.2f')

test.loc[test['leak'].notnull(), 'target'] = test.loc[test['leak'].notnull(), 'leak']
test[['ID', 'target']].to_csv('submission.csv', index=False, float_format='%.2f')