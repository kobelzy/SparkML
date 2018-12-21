package org.lzy.kaggle.eloRecommendation

import java.sql.Timestamp

import common.{DataUtils, SparkUtil}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

object DataCollect {

  /**
    * train:first_active_month,card_id,feature_1,feature_2,feature_3,target
    * trancaction:  authorized_flag,card_id        ,city_id,category_1,installments,category_3,merchant_category_id,merchant_id    ,month_lag,purchase_amount,purchase_date      ,category_2,state_id,subsector_id
    * merchant:  merchant_id,merchant_group_id,merchant_category_id,subsector_id,numerical_1,numerical_2,category_1,most_recent_sales_range,most_recent_purchases_range,avg_sales_lag3,avg_purchases_lag3,active_months_lag3,avg_sales_lag6,
    * avg_purchases_lag6,active_months_lag6,avg_sales_lag12,avg_purchases_lag12,active_months_lag12,category_4,city_id,state_id,category_2
    */
  case class caseAll(card_id: String, first_active_month: String, feature_1: Int, feature_2: Int, feature_3: Int,
                     merchant_group_id: String, numerical_1: Double, numerical_2: Double, category_1_merchant: String, category_2_merchant: Double, category_4_merchant: String, city_id_merchant: String, state_id_merchant: String, subsector_id_merchant: String,
                     most_recent_sales_range: String, most_recent_purchases_range: String, avg_sales_lag3: Double, avg_purchases_lag3: Double, active_months_lag3: Int, avg_sales_lag6: Double, avg_purchases_lag6: Double, active_months_lag6: Int, avg_sales_lag12: Double, avg_purchases_lag12: Double, active_months_lag12: Int,
                     authorized_flag: String, city_id: String, category_1: String, installments: Int, category_3: String, merchant_category_id: String, merchant_id: String, month_lag: Int, purchase_amount: Double, purchase_date: Timestamp, category_2: Double, state_id: String, subsector_id: String,
                     target: Double
                    )

  case class caseTransactions(authorized_flag: Int, card_id: String, city_id: String, category_1: Option[Double], installments: Int, category_3: String, merchant_category_id: String, merchant_id: String,
                              month_lag: Int, purchase_amount: Double, purchase_date: Timestamp, category_2: Option[Double], state_id: String, subsector_id: String,
                              diff_month: Double,  week: Double,month: Double)

  case class caseBase(first_active_month: String, card_id: String, feature_1: Int, feature_2: Int, feature_3: Int, target: Double)

  case class abc(card_id: String, feature_1_size: Double, feature_1_max: Double, feature_1_min: Double, feature_1_mean: Double)


  val spark = Run.spark
  val utils = new DataUtils(spark)

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    //    val (new_feature_df,authorized_feature_df,history_df)= collectTransaction
    //    new_feature_df.show(false)
    //    val   (train_df,test_df) =extractTranAndTest

    //    val train=train_df.join(new_feature_df,Seq("card_id"),"left")
    //      .join(authorized_feature_df,Seq("card_id"),"left")
    //      .join(history_df,Seq("card_id"),"left")

    //    val test=test_df.join(new_feature_df,$"card_id","left")
    //      .join(authorized_feature_df,$"card_id","left")
    //      .join(history_df,$"card_id","left")


    //    train.show(false)
    //    train.printSchema()
  }

  def extractTranAndTest(trainPaht:String,testPath:String) = {
    val train_df = utils.read_csv(trainPaht)
    val test_df = utils.read_csv(testPath)
      .withColumn("target", lit(0d))
    (train_df, test_df)
  }

  def collectTransaction(historicalPath: String, newPath: String) = {
    val merchants_df = utils.read_csv(EloConstants.merchants)
      .select($"merchant_group_id", $"merchant_category_id", $"numerical_1", $"numerical_2",
        $"category_1".alias("category_1_merchant"), $"category_2".alias("category_2_merchant"), $"category_4".alias("category_4_merchant"),
        $"city_id".alias("city_id_merchant"), $"state_id".alias("state_id_merchant"), $"subsector_id".alias("subsector_id_merchant"),
        $"most_recent_sales_range", $"most_recent_purchases_range", $"avg_sales_lag3", $"avg_purchases_lag3", $"active_months_lag3",
        $"avg_sales_lag6", $"avg_purchases_lag6", $"active_months_lag6", $"avg_sales_lag12", $"avg_purchases_lag12", $"active_months_lag12"
      )


    val new_ds = transformTransaction(utils.read_csv(newPath)).repartition(100)
    val historyTransaction_ds: Dataset[caseTransactions] = transformTransaction(utils.read_csv(historicalPath)).repartition(100)
    val authorized_ds = historyTransaction_ds.filter(_.authorized_flag == 1)
    val history_ds = historyTransaction_ds.filter(_.authorized_flag == 0)
    //    historyTransaction_ds.show(false)
    //    historyTransaction_ds.printSchema()
    //    val all_df=train_df      .join(transactions_df,"card_id")
    //      .join(merchants_df,"merchant_category_id")
    //    all_df.printSchema()

    val new_feature_df = extractFeatureFromTransaction(new_ds, "new_")
    val authorized_feature_df = extractFeatureFromTransaction(authorized_ds, "auth_")
    val history_df = extractFeatureFromTransaction(history_ds, "hist_")
    (new_feature_df, authorized_feature_df, history_df)
  }


  def time2Long=udf{time:Timestamp=>time.getTime}
  /**
    * 转换transaction交易数据为指定ds
    *
    * @param transaction_df
    * @return
    */
  def transformTransaction(transaction_df: DataFrame) = {
    transaction_df
      .withColumn("authorized_flag", when($"authorized_flag" === "Y", 1).otherwise(0))
      .withColumn("category_1", when($"category_1" === "Y", 1d).otherwise(0d))
      .withColumn("diff_month", datediff(current_timestamp(), $"purchase_date") / 30)
      .withColumn("week", dayofweek($"purchase_date"))
      .withColumn("month", month($"purchase_date"))
      .withColumn("purchase_date",time2Long($"purchase_date"))

      .as[caseTransactions]
  }

  /**
    * 抽取交易数据的特征
    *
    * @param transaction_ds
    * @param suffix
    * @return
    */
  def extractFeatureFromTransaction(transaction_ds: Dataset[caseTransactions], suffix: String) = {
    /*
    按照月份+card进行分组的，和月份有关的统计项有：installments，purchase_amount
     */
    val im_purchase_ds = transaction_ds.groupBy("card_id", "month_lag")

      .agg(sum("installments").alias("installments_sum"), min("installments").alias("installments_min"),
        max("installments").alias("installments_max"), stddev("installments").alias("installments_std"),
        sum("purchase_amount").alias("purchase_amount_sum"), min("purchase_amount").alias("purchase_amount_min"),
        max("purchase_amount").alias("purchase_amount_max"), stddev("purchase_amount").alias("purchase_amount_std")
      ).na.fill(0d)
      .groupBy("card_id")
      .agg(stddev("installments_sum").alias("installments_sum_stddev"), avg("installments_sum").alias("installments_sum_avg"),
        stddev("installments_min").alias("installments_min_stddev"), avg("installments_min").alias("installments_min_avg"),
        stddev("installments_max").alias("installments_max_stddev"), avg("installments_max").alias("installments_max_avg"),
        stddev("installments_std").alias("installments_std_stddev"), avg("installments_std").alias("installments_std_avg"),
        stddev("purchase_amount_sum").alias("purchase_amount_sum_stddev"), avg("purchase_amount_sum").alias("purchase_amount_sum_avg"),
        stddev("purchase_amount_min").alias("purchase_amount_min_stddev"), avg("purchase_amount_min").alias("purchase_amount_min_avg"),
        stddev("purchase_amount_max").alias("purchase_amount_max_stddev"), avg("purchase_amount_max").alias("purchase_amount_max_avg"),
        stddev("purchase_amount_std").alias("purchase_amount_std_stddev"), avg("purchase_amount_std").alias("purchase_amount_std_avg")
      ).na.fill(0d)


//    historical_transactions['month_diff'] = ((datetime.datetime.today() - historical_transactions['purchase_date']).dt.days)//30
//    historical_transactions['month_diff'] += historical_transactions['month_lag']
//
//    new_transactions['month_diff'] = ((datetime.datetime.today() - new_transactions['purchase_date']).dt.days)//30
//    new_transactions['month_diff'] += new_transactions['month_lag']
    val transaction_card_ds = transaction_ds.groupBy("card_id")
      .agg(avg("category_1").alias("category_1_avg"), stddev("category_1").alias("category_1_std"),
        avg("category_2").alias("category_2_avg"), stddev("category_2").alias("category_2_std"),
        countDistinct("city_id").alias("city_id_count"), countDistinct("state_id").alias("state_id_count"),
        countDistinct("subsector_id").alias("subsector_id_count"), avg("month_lag").alias("month_lag_avg"),
        mean("diff_month").alias("diff_month_avg"), avg("week").alias("week_avg"), countDistinct("month").as("count_month"),
        max("purchase_date").alias("purchase_date_max"),min("purchase_date").alias("purchase_date_min"),
        sort_array(collect_list("purchase_date")).alias("purchase_date_list")
      )
      .na.fill(0d)

    /*    transaction_ds.groupByKey(_.card_id)
          .mapGroups { case (card_id, iter_) =>
            val iter = iter_.toIterable
            val count: Double = iter.size.toDouble
            val category_1_sum = iter.map(_.category_1.getOrElse(0d)).sum
            val category_1_mean = category_1_sum / count

            val category_2_sum = iter.map(_.category_2.getOrElse(0d)).sum
            val category_2_mean = category_2_sum / count

            //category_3是A->E的数据
            //                val category_3_sum=iter.map(_.category_3).sum
            //                val category_3_mean=category_3_sum/count
            //                val category_3_max=iter.map(_.category_3).max
            //                val category_3_min=iter.map(_.category_3).min


            val city_num = iter.map(_.city_id).toSet.size
            val state_num = iter.map(_.state_id).toSet.size
            val subsector_num = iter.map(_.subsector_id).toSet.size

            val months = iter.map(_.month_lag).sum / count
            (card_id, months, count, category_1_sum, category_1_mean, category_2_sum, category_2_mean, city_num, state_num, subsector_num)

        })*/
    val joined_df = im_purchase_ds.join(transaction_card_ds, "card_id")

    joined_df.columns.filterNot(_.equals("card_id")).foldLeft(joined_df)((current, c) => {
      current.withColumnRenamed(c, suffix + c)
    })
  }


}

