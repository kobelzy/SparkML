package org.lzy.kaggle.eloRecommendation

import java.sql.Timestamp
import java.time.{Duration, LocalDateTime}

import common.{DataUtils, SparkUtil}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}
import org.lzy.kaggle.eloRecommendation.EloConstants.basePath
import org.apache.spark.sql.functions._

object DataCollect {
  val spark = SparkUtil.getSpark()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    collect
  }

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
                              month_lag: Int, purchase_amount: Double, purchase_date: Timestamp, category_2: Option[Double], state_id: String, subsector_id: String)

  case class caseBase(first_active_month: String, card_id: String, feature_1: Int, feature_2: Int, feature_3: Int, target: Double)

  case class abc(card_id: String, feature_1_size: Double, feature_1_max: Double, feature_1_min: Double, feature_1_mean: Double)

  def collect() = {
    val utils = new DataUtils(spark)
    val train_df = utils.read_csv(EloConstants.trainPath)
    val merchants_df = utils.read_csv(EloConstants.merchants)
      .select($"merchant_group_id", $"merchant_category_id", $"numerical_1", $"numerical_2",
        $"category_1".alias("category_1_merchant"), $"category_2".alias("category_2_merchant"), $"category_4".alias("category_4_merchant"), $"city_id".alias("city_id_merchant"), $"state_id".alias("state_id_merchant"), $"subsector_id".alias("subsector_id_merchant"),
        $"most_recent_sales_range", $"most_recent_purchases_range", $"avg_sales_lag3", $"avg_purchases_lag3", $"active_months_lag3", $"avg_sales_lag6", $"avg_purchases_lag6", $"active_months_lag6", $"avg_sales_lag12", $"avg_purchases_lag12", $"active_months_lag12"
      )


    val new_ds = transformTransaction(utils.read_csv(EloConstants.newMerChantTransactions_mini))
    val historyTransaction_ds: Dataset[caseTransactions] = transformTransaction(utils.read_csv(EloConstants.historical_mini))

    //    val authorized_ds = historyTransaction_ds.filter(_.authorized_flag == 1)
    //    val history_ds = historyTransaction_ds.filter(_.authorized_flag == 0)
    //    historyTransaction_ds.show(false)
    //    historyTransaction_ds.printSchema()
    //    val all_df=train_df      .join(transactions_df,"card_id")
    //      .join(merchants_df,"merchant_category_id")
    historyTransaction_ds.show(false)
    //    all_df.printSchema()

    extractFeatureFromTransaction(historyTransaction_ds).show(false)
  }


  /**
    * 转换transaction交易数据为指定ds
    *
    * @param transaction_df
    * @return
    */
  def transformTransaction(transaction_df: DataFrame) = {
    transaction_df.withColumn("authorized_flag", when($"authorized_flag" === "Y", 1).otherwise(0))
      .withColumn("category_1", when($"category_1" === "Y", 1d).otherwise(0d))
      .as[caseTransactions]

  }

  def extractFeatureFromTransaction(transaction_ds: Dataset[caseTransactions]) = {
    transaction_ds.groupBy("card_id","month_lag")
      .agg("installments"->"sum","installments"->"mean","installments"->"min","installments"->"max","installments"->"std",
        "purchase_amount"->"sum","purchase_amount"->"mean","purchase_amount"->"min","purchase_amount"->"max","purchase_amount"->"std"
      )
      .groupBy("card_id")
      .pivot()
    //    (authorized_flag:Int,card_id:String, city_id:String, category_1:Int, installments:Int, category_3:String, merchant_category_id:String, merchant_id:String,
    //      month_lag:Int, purchase_amount:Double, purchase_date :Timestamp, category_2:Double, state_id:String, subsector_id:String)
    /*
    按照月份+card进行分组的，和月份有关的统计项有：installments，purchase_amount
     */
       val transaction_card_month_ds= transaction_ds.groupByKey(t => t.card_id+"_"+t.month_lag)
          .mapGroups { case (card_id2month_lag, iter) =>
            val count: Double = iter.length.toDouble

            val installments_sum = iter.map(_.installments).sum.toDouble
            val installments_mean = installments_sum / count
            val installments_min = iter.map(_.installments).min.toDouble
            val installments_max = iter.map(_.installments).max.toDouble

            val purchase_amount_sum = iter.map(_.purchase_amount).sum
            val purchase_amount_mean = purchase_amount_sum / count
            val purchase_amount_min = iter.map(_.purchase_amount).min
            val purchase_amount_max = iter.map(_.purchase_amount).max

            val card_id=card_id2month_lag.split("_").head
            (card_id,count,installments_sum ,installments_mean,installments_min,installments_max,purchase_amount_sum ,purchase_amount_mean,purchase_amount_min,purchase_amount_max )
          }
        .groupByKey(_._1)
        .mapGroups{case (card_id,iter)=>
        //对每个指标计算平均值以及std
        }



    transaction_ds.rdd.map(t=>(t.month_lag,t)).groupByKey()
    val transaction_card_ds
    = transaction_ds.groupByKey(_.authorized_flag)
      .mapGroups { case (card_id, iter_) =>
    val iter=iter_.toIterable
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


        val purchase_months = iter.map(_.purchase_date.toLocalDateTime.getMonthValue.toDouble - 2)
        val purchase_months_sum = purchase_months.sum
        val purchase_months_mean = purchase_months_sum / count
        val purchase_months_min=purchase_months.min
        val purchase_months_max=purchase_months.max


        val city_num = iter.map(_.city_id).toSet.size
        val state_num = iter.map(_.state_id).toSet.size
        val subsector_num = iter.map(_.subsector_id).toSet.size

        val months = iter.map(_.month_lag).sum / count
        (card_id, months, count, category_1_sum, category_1_mean, category_2_sum, category_2_mean, purchase_months_sum, purchase_months_mean, city_num, state_num, subsector_num,purchase_months_min,purchase_months_max)
      }
    transaction_card_ds
    //    val aff_map=Map(())
    //    transaction_car_month_ds.groupBy("card_id").agg()
  }

  /**
    * * train:first_active_month,card_id,feature_1,feature_2,feature_3,target
    *
    * @param base_df
    * @return
    */
  def extractFeatureFromTranAndTest(base_df: DataFrame) = {
    val test: Dataset[(String, Double, Double, Double, Double)] = base_df.as[caseBase]
      .groupByKey(_.card_id)
      .mapGroups { case (card_id, iter) =>
        val feature_1_size = iter.map(_.feature_1).size.toDouble
        val feature_1_max = iter.map(_.feature_1).max.toDouble
        val feature_1_min = iter.map(_.feature_1).min.toDouble
        val feature_1_mean = iter.map(_.feature_1).sum / feature_1_size
        (card_id, feature_1_size, feature_1_max, feature_1_min, feature_1_mean)
      }


  }


}

