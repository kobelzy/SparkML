##Elo Merchant Category Recommendation
该比赛是巴西的一个信用卡公司，

##源数据
###train （201918条）

 - card_id	信用卡唯一标识符
 - first_active_month	购买的记录时间，精确到月，格式为：'YYYY-MM'
 - feature_1	脱敏特征1
 - feature_2	脱敏特征2
 - feature_3	脱敏特征3
 - target	在2个月之后的数据评分。Loyalty numerical score calculated 2 months after historical and evaluation period

...

    +------------------+---------------+---------+---------+---------+-----------+
    |first_active_month|card_id        |feature_1|feature_2|feature_3|target     |
    +------------------+---------------+---------+---------+---------+-----------+
    |2017-06           |C_ID_92a2005557|5        |2        |1        |-0.8202826 |
    |2017-01           |C_ID_3d0044924f|4        |1        |0        |0.39291325 |
    |2016-08           |C_ID_d639edf6cd|2        |2        |0        |0.68805599 |
    +------------------+---------------+---------+---------+---------+-----------+
    
###test (123624条)
与train结构一致，没有target项

    +------------------+---------------+---------+---------+---------+
    |first_active_month|card_id        |feature_1|feature_2|feature_3|
    +------------------+---------------+---------+---------+---------+
    |2017-04           |C_ID_0ab67a22ab|3        |3        |1        |
    |2017-01           |C_ID_130fd0cbdd|2        |3        |0        |
    |2017-08           |C_ID_b709037bc5|5        |1        |1        |
    +------------------+---------------+---------+---------+---------+
###historical_transaction（29112361，2千900万）
信用卡过去三个月的消费记录
 - card_id	
 - month_lag	距离参考日期的月份间隔
 - purchase_date	购买时间
 - authorized_flag	置信标签，Y是经过确认的，N是没有的
 - installments	分期付款期数
 - merchant_category_id	客户商品id (anonymized )
 - subsector_id	客户商品种类id (anonymized )
 - merchant_id	客户id（脱敏）
 - purchase_amount	正则化后的购买数量
 - city_id	城市脱敏id
 - state_id	州脱敏id
 - category_1	脱敏商品1
 - category_2	脱敏商品2
 - category_3	脱敏商品3

...

    +---------------+---------------+-------+----------+------------+----------+--------------------+---------------+---------+---------------+-------------------+----------+--------+------------+
    |authorized_flag|card_id        |city_id|category_1|installments|category_3|merchant_category_id|merchant_id    |month_lag|purchase_amount|purchase_date      |category_2|state_id|subsector_id|
    +---------------+---------------+-------+----------+------------+----------+--------------------+---------------+---------+---------------+-------------------+----------+--------+------------+
    |Y              |C_ID_4e6213e9bc|88     |N         |0           |A         |80                  |M_ID_e020e9b302|-8       |-0.70333091    |2017-06-25 15:33:07|1.0       |16      |37          |
    |Y              |C_ID_4e6213e9bc|88     |N         |0           |A         |367                 |M_ID_86ec983688|-7       |-0.73312848    |2017-07-15 12:10:45|1.0       |16      |16          |
    |Y              |C_ID_4e6213e9bc|88     |N         |0           |A         |80                  |M_ID_979ed661fc|-6       |-0.720386      |2017-08-09 22:04:29|1.0       |16      |37          |
    |Y              |C_ID_4e6213e9bc|88     |N         |0           |A         |560                 |M_ID_e6d5ae8ea6|-5       |-0.73535241    |2017-09-02 10:06:26|1.0       |16      |34          |
    +---------------+---------------+-------+----------+------------+----------+--------------------+---------------+---------+---------------+-------------------+----------+--------+------------+

###new_merchant_period（196303，196万行）
信用卡当前月的消费记录
结构与historical一致

    +---------------+---------------+-------+----------+------------+----------+--------------------+---------------+---------+---------------+-------------------+----------+--------+------------+
    |authorized_flag|card_id        |city_id|category_1|installments|category_3|merchant_category_id|merchant_id    |month_lag|purchase_amount|purchase_date      |category_2|state_id|subsector_id|
    +---------------+---------------+-------+----------+------------+----------+--------------------+---------------+---------+---------------+-------------------+----------+--------+------------+
    |Y              |C_ID_415bb3a509|107    |N         |1           |B         |307                 |M_ID_b0c793002c|1        |-0.55757375    |2018-03-11 14:57:36|1.0       |9       |19          |
    |Y              |C_ID_415bb3a509|140    |N         |1           |B         |307                 |M_ID_88920c89e8|1        |-0.56957993    |2018-03-19 18:53:37|1.0       |9       |19          |
    |Y              |C_ID_415bb3a509|330    |N         |1           |B         |507                 |M_ID_ad5237ef6b|2        |-0.55103721    |2018-04-26 14:08:44|1.0       |9       |14          |
    +---------------+---------------+-------+----------+------------+----------+--------------------+---------------+---------+---------------+-------------------+----------+--------+------------+

###merchants(334697)
 - merchant_id	商家id
 - merchant_group_id	商家分组id
 - merchant_category_id	商家商品id（脱敏）
 - city_id	City identifier (anonymized )
 - state_id	State identifier (anonymized )
 - subsector_id 商品分类
 - numerical_1	脱敏指标1
 - numerical_2	脱敏指标2
 - category_1	脱敏商品1
 - category_2	脱敏商品2
 - category_4	脱敏商品4
 - most_recent_sales_range	Range of revenue (monetary units) in last active month --> A > B > C > D > E
 - most_recent_purchases_range	Range of quantity of transactions in last active month --> A > B > C > D > E
 - avg_sales_lag3	过去3个月的月平均收入除以上一个活跃月份的收入 ,Monthly average of revenue in last 3 months divided by revenue in last active month
 - avg_purchases_lag3	最后3个月的每月平均交易除以上一个活跃月份的交易数 ,Monthly average of transactions in last 3 months divided by transactions in last active month
 - active_months_lag3	过去3个月内活跃月份的数量,Quantity of active months within last 3 months
 - avg_sales_lag6	过去6个月的月平均收入除以上一个活跃月份的收入,Monthly average of revenue in last 6 months divided by revenue in last active month
 - avg_purchases_lag6	过去6个月的每月平均交易额除以上一个活跃月份的交易数,Monthly average of transactions in last 6 months divided by transactions in last active month
 - active_months_lag6	过去6个月内活跃月份的数量Quantity of active months within last 6 months
 - avg_sales_lag12	过去12个月的月平均收入除以上一个活跃月份的收入,Monthly average of revenue in last 12 months divided by revenue in last active month
 - avg_purchases_lag12	过去12个月的每月平均交易额除以上一个活跃月份的交易数,Monthly average of transactions in last 12 months divided by transactions in last active month
 - active_months_lag12	过去12个月内活跃月份的数量,Quantity of active months within last 12 months

...

    +---------------+-----------------+--------------------+------------+-----------+-----------+----------+-----------------------+---------------------------+--------------+------------------+------------------+--------------+------------------+------------------+---------------+-------------------+-------------------+----------+-------+--------+----------+
    |merchant_id    |merchant_group_id|merchant_category_id|subsector_id|numerical_1|numerical_2|category_1|most_recent_sales_range|most_recent_purchases_range|avg_sales_lag3|avg_purchases_lag3|active_months_lag3|avg_sales_lag6|avg_purchases_lag6|active_months_lag6|avg_sales_lag12|avg_purchases_lag12|active_months_lag12|category_4|city_id|state_id|category_2|
    +---------------+-----------------+--------------------+------------+-----------+-----------+----------+-----------------------+---------------------------+--------------+------------------+------------------+--------------+------------------+------------------+---------------+-------------------+-------------------+----------+-------+--------+----------+
    |M_ID_838061e48c|8353             |792                 |9           |-0.05747065|-0.05747065|N         |E                      |E                          |-0.4          |9.66666667        |3                 |-2.25         |18.66666667       |6                 |-2.32          |13.91666667        |12                 |N         |242    |9       |1.0       |
    |M_ID_9339d880ad|3184             |840                 |20          |-0.05747065|-0.05747065|N         |E                      |E                          |-0.72         |1.75000000        |3                 |-0.74         |1.29166667        |6                 |-0.57          |1.68750000         |12                 |N         |22     |16      |1.0       |
    |M_ID_e726bbae1e|447              |690                 |1           |-0.05747065|-0.05747065|N         |E                      |E                          |-82.13        |260.00000000      |2                 |-82.13        |260.00000000      |2                 |-82.13         |260.00000000       |2                  |N         |-1     |5       |5.0       |
    |M_ID_a70e9c5f81|5026             |792                 |9           |-0.05747065|-0.05747065|Y         |E                      |E                          |null          |1.66666667        |3                 |null          |4.66666667        |6                 |null           |3.83333333         |12                 |Y         |-1     |-1      |null      |
    +---------------+-----------------+--------------------+------------+-----------+-----------+----------+-----------------------+---------------------------+--------------+------------------+------------------+--------------+------------------+------------------+---------------+-------------------+-------------------+----------+-------+--------+----------+
    


