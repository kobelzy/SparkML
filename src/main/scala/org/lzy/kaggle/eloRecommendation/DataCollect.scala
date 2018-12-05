package org.lzy.kaggle.eloRecommendation

import common.{SparkUtils, Utils}
import org.lzy.kaggle.eloRecommendation.EloConstants.basePath

object DataCollect {
  def main(args: Array[String]): Unit = {
    collect
  }

  /**
    * train:first_active_month,card_id,feature_1,feature_2,feature_3,target
    * trancaction:  authorized_flag,card_id        ,city_id,category_1,installments,category_3,merchant_category_id,merchant_id    ,month_lag,purchase_amount,purchase_date      ,category_2,state_id,subsector_id
    * merchant:  merchant_id,merchant_group_id,merchant_category_id,subsector_id,numerical_1,numerical_2,category_1,most_recent_sales_range,most_recent_purchases_range,avg_sales_lag3,avg_purchases_lag3,active_months_lag3,avg_sales_lag6,
    * avg_purchases_lag6,active_months_lag6,avg_sales_lag12,avg_purchases_lag12,active_months_lag12,category_4,city_id,state_id,category_2
    */

  case class caseAll(card_id:String,first_active_month:String,feature_1:Int,feature_2:Int,feature_3:Int,
                     merchant_group_id:String,numerical_1:Int,numerical_2:Int,merchant_category_1:Int,merchant_category_2:Int,merchant_category_4:Int,merchant_city_id:Int,merchant_state_id:Int,merchant_subsector_id:Int,
                     most_recent_sales_range:Int,most_recent_purchases_range:Int,avg_sales_lag3:Int,avg_purchases_lag3:Int,active_months_lag3:Int,avg_sales_lag6:Int,avg_purchases_lag6:Int,active_months_lag6:Int,avg_sales_lag12:Int,avg_purchases_lag12:Int,active_months_lag12:Int,
                     authorized_flag:String        ,city_id:String,category_1:Int,installments:Int,category_3:Int,merchant_category_id:Int,merchant_id:String    ,month_lag:Int,purchase_amount:Int,purchase_date :String     ,category_2:Int,state_id:Int,subsector_id:Int
                    )
  def collect()={
  val spark=SparkUtils.getSpark("collect")
    import spark.implicits._
    val utils=new Utils(spark)

    val train_df=utils.readToCSV(EloConstants.trainPath)
    val merchants_df=utils.readToCSV(EloConstants.merchants)
      .select($"merchant_group_id",$"merchant_category_id",$"numerical_1",$"numerical_2",
        $"category_1".alias("merchant_category_1"),$"category_2".alias("merchant_category_2"),$"category_4".alias("merchant_category_4"),$"city_id".alias("merchant_city_id"),$"state_id".alias("merchant_state_id"),$"subsector_id".alias("merchant_subsector_id"),
        $"most_recent_sales_range",$"most_recent_purchases_range",$"avg_sales_lag3",$"avg_purchases_lag3",$"active_months_lag3",$"avg_sales_lag6",$"avg_purchases_lag6",$"active_months_lag6",$"avg_sales_lag12",$"avg_purchases_lag12",$"active_months_lag12"
      )
    val newTransactions_df=utils.readToCSV(EloConstants.newMerChantTransactions_mini)
    val historyTransactions_df=utils.readToCSV(EloConstants.historical_mini)
    val transactions_df=newTransactions_df.union(historyTransactions_df)


    val all_df=train_df      .join(transactions_df,"card_id")
      .join(merchants_df,"merchant_category_id")
all_df.show(false)


  }


}
