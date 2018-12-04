package org.lzy.kaggle.eloRecommendation

import common.{SparkUtils, Utils}
import org.apache.spark.sql.functions._
object DataExplore {
  def main(args: Array[String]): Unit = {
//  run1()
    merchantsExplor()
  }


  def run1()={
    val spark=SparkUtils.getSpark("elo")
    val utils=new Utils(spark)


    val train_df=utils.readToCSV(EloConstants.trainPath)

    println("train_df长度："+train_df.count())
    train_df.show(false)

    val test_df=utils.readToCSV(EloConstants.testPath)

    println("test_df长度："+test_df.count())
    test_df.show(false)


    val newMerChantTransactions_df=utils.readToCSV(EloConstants.newMerChantTransactions)

    println("newMerChantTransactions_df长度："+newMerChantTransactions_df.count())
    newMerChantTransactions_df.show(false)


    val historical_df=utils.readToCSV(EloConstants.historical)

    println("historical_df长度："+historical_df.count())
    historical_df.show(false)


    val merchants_df=utils.readToCSV(EloConstants.merchants)

    println("merchants长度："+merchants_df.count())
    merchants_df.show(false)



  }

  def merchantsExplor()={
    val spark=SparkUtils.getSpark("elo")
    val utils=new Utils(spark)
    val merchants_df=utils.readToCSV(EloConstants.merchants)

    println("merchants长度："+merchants_df.count())
    merchants_df.show(false)

//    println("去重后数量："+merchants_df.select("merchant_id").distinct().count())
    val ThanOneMerchants=merchants_df.groupBy("merchant_id").count().filter("count > 1").select("merchant_id").collect().map(_.getString(0))
    merchants_df.filter(row=>ThanOneMerchants.contains(row.getString(0))).sort("merchant_id").show(false)

  }
}
