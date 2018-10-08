import functools
import gc
import logging
import numpy as np
import warnings
from multiprocessing import Pool as Pool
from pandas.core.common import SettingWithCopyWarning
import pandas as pd

def get_keys_for_field(field=None):
    the_dict = {
        'device': [
            'browser',
            'browserSize',
            'browserVersion',
            'deviceCategory',
            'flashVersion',
            'isMobile',
            'language',
            'mobileDeviceBranding',
            'mobileDeviceInfo',
            'mobileDeviceMarketingName',
            'mobileDeviceModel',
            'mobileInputSelector',
            'operatingSystem',
            'operatingSystemVersion',
            'screenColors',
            'screenResolution'
        ],
        'geoNetwork': [
            'city',
            'cityId',
            'continent',
            'country',
            'latitude',
            'longitude',
            'metro',
            'networkDomain',
            'networkLocation',
            'region',
            'subContinent'
        ],
        'totals': [
            'bounces',
            'hits',
            'newVisits',
            'pageviews',
            'transactionRevenue',
            'visits'
        ],
        'trafficSource': [
            'adContent',
            'adwordsClickInfo',
            'campaign',
            'campaignCode',
            'isTrueDirect',
            'keyword',
            'medium',
            'referralPath',
            'source'
        ],
    }

    return the_dict[field]


def apply_func_on_series(data=None, func=None):
    return data.apply(lambda x: func(x))


def multi_apply_func_on_series(df=None, func=None, n_jobs=4):
    p = Pool(n_jobs)
    f_ = p.map(functools.partial(apply_func_on_series, func=func),
               np.array_split(df, n_jobs))
    f_ = pd.concat(f_, axis=0, ignore_index=True)
    p.close()
    p.join()
    return f_.values


def convert_to_dict(x):
    return eval(x.replace('false', 'False')
                .replace('true', 'True')
                .replace('null', 'np.nan'))


def get_dict_field(x_, key_):
    try:
        return x_[key_]
    except KeyError:
        return np.nan


def develop_json_fields(df=None):
    json_fields = ['device', 'geoNetwork', 'totals', 'trafficSource']
    # Get the keys
    for json_field in json_fields:
        # print('Doing Field {}'.format(json_field))
        # Get json field keys to create columns
        the_keys = get_keys_for_field(json_field)
        # Replace the string by a dict
        # print('Transform string to dict')
        df[json_field] = multi_apply_func_on_series(
            df=df[json_field],
            func=convert_to_dict,
            n_jobs=4
        )
        logger.info('{} converted to dict'.format(json_field))
        #         df[json_field] = df[json_field].apply(lambda x: eval(x
        #                                             .replace('false', 'False')
        #                                             .replace('true', 'True')
        #                                             .replace('null', 'np.nan')))
        for k in the_keys:
            # print('Extracting {}'.format(k))
            df[json_field + '_' + k] = df[json_field].apply(lambda x: get_dict_field(x_=x, key_=k))
        del df[json_field]
        gc.collect()
        logger.info('{} fields extracted'.format(json_field))
    return df


def main(nrows=None):
    basePath = "D:/Dataset/GoogleAnalytics/"

    # Convert train
    train = pd.read_csv(basePath + 'source/train.csv', dtype='object', nrows=nrows, encoding='utf-8')
    train = develop_json_fields(df=train)
    logger.info('Train done')

    # Convert test
    test = pd.read_csv(basePath + 'source/test.csv', dtype='object', nrows=nrows, encoding='utf-8')
    test = develop_json_fields(df=test)
    logger.info('Test done')

    # Check features validity
    for f in train.columns:
        if f not in ['date', 'fullVisitorId', 'sessionId']:
            try:
                train[f] = train[f].astype(np.float64)
                test[f] = test[f].astype(np.float64)
            except (ValueError, TypeError):
                logger.info('{} is a genuine string field'.format(f))
                pass
            except Exception:
                logger.exception('{} enountered an exception'.format(f))
                raise

    logger.info('{}'.format(train['totals_transactionRevenue'].sum()))
    feature_to_drop = []
    for f in train.columns:
        if f not in ['date', 'fullVisitorId', 'sessionId', 'totals_transactionRevenue']:
            if train[f].dtype == 'object':
                try:
                    trn, _ = pd.factorize(train[f])
                    tst, _ = pd.factorize(test[f])
                    if (np.std(trn) == 0) | (np.std(tst) == 0):
                        feature_to_drop.append(f)
                        logger.info('No variation in {}'.format(f))
                except TypeError:
                    feature_to_drop.append(f)
                    logger.info('TypeError exception for {}'.format(f))
            else:
                if (np.std(train[f].fillna(0).values) == 0) | (np.std(test[f].fillna(0).values) == 0):
                    feature_to_drop.append(f)
                    logger.info('No variation in {}'.format(f))
    test.drop(feature_to_drop, axis=1, inplace=True)
    train.drop(feature_to_drop, axis=1, inplace=True)
    logger.info('{}'.format(train['totals_transactionRevenue'].sum()))

    for f in train.columns:
        if train[f].dtype == 'object':
            train[f] = train[f].apply(lambda x: try_encode(x))
            test[f] = test[f].apply(lambda x: try_encode(x))

    test.to_csv(basePath + 'source/extracted_fields_test.csv', index=False)
    train.to_csv(basePath + 'source/extracted_fields_train.csv', index=False)


def try_encode(x):
    """Used to remove any encoding issues within the data"""
    try:
        return x.encode('utf-8', 'surrogateescape').decode('utf-8')
    except AttributeError:
        return np.nan
    except UnicodeEncodeError:
        return np.nan


def get_logger():
    logger_ = logging.getLogger('main')
    logger_.setLevel(logging.DEBUG)
    fh = logging.FileHandler('logging.log')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(levelname)s]%(asctime)s:%(name)s:%(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger_.addHandler(fh)
    logger_.addHandler(ch)

    return logger_


if __name__ == '__main__':
    logger = get_logger()
    try:
        warnings.simplefilter('error', SettingWithCopyWarning)
        gc.enable()
        logger.info('Process started')
        main(nrows=None)
    except Exception as err:
        logger.exception('Exception occured')
        raise

