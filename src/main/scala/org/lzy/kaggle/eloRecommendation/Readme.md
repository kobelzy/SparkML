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

###test (123624条)
与train结构一致，没有target项

###historical_transaction（29112361，2千900万）

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
 - category_2	脱敏商品
 - category_3	脱敏商品3

###new_merchant_period（196303，196万行）
结构与historical一致

###merchants(334697)
 - merchant_id	客户id
 - merchant_group_id	客户分组id
 - merchant_category_id	客户商品id（脱敏）
 - subsector_id 商品分类
 - numerical_1	脱敏指标
 - numerical_2	脱敏指标
 - category_1	脱敏商品
 - most_recent_sales_range	Range of revenue (monetary units) in last active month --> A > B > C > D > E
 - most_recent_purchases_range	Range of quantity of transactions in last active month --> A > B > C > D > E
 - avg_sales_lag3	Monthly average of revenue in last 3 months divided by revenue in last active month
 - avg_purchases_lag3	Monthly average of transactions in last 3 months divided by transactions in last active month
 - active_months_lag3	Quantity of active months within last 3 months
 - avg_sales_lag6	Monthly average of revenue in last 6 months divided by revenue in last active month
 - avg_purchases_lag6	Monthly average of transactions in last 6 months divided by transactions in last active month
 - active_months_lag6	Quantity of active months within last 6 months
 - avg_sales_lag12	Monthly average of revenue in last 12 months divided by revenue in last active month
 - avg_purchases_lag12	Monthly average of transactions in last 12 months divided by transactions in last active month
 - active_months_lag12	Quantity of active months within last 12 months
 - category_4	anonymized category
 - city_id	City identifier (anonymized )
 - state_id	State identifier (anonymized )
 - category_2	anonymized category
