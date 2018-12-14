package org.lzy.kaggle.eloRecommendation

import common.{DataUtils, SparkUtil}
import org.apache.spark.sql.functions._
import org.apache.spark.util.SparkUtils
object DataExplore {
  def main(args: Array[String]): Unit = {
//  run1()
//    merchantsExplor()
//    explorNewAndHistory
    exploreCardId
  }


  def run1()={
    val spark=SparkUtil.getSpark()
    val utils=new DataUtils(spark)


    val train_df=utils.read_csv(EloConstants.trainPath)

    println("train_df长度："+train_df.count())
    train_df.show(false)

    val test_df=utils.read_csv(EloConstants.testPath)

    println("test_df长度："+test_df.count())
    test_df.show(false)


    val newMerChantTransactions_df=utils.read_csv(EloConstants.newMerChantTransactions)

    println("newMerChantTransactions_df长度："+newMerChantTransactions_df.count())
    newMerChantTransactions_df.show(false)


    val historical_df=utils.read_csv(EloConstants.historical)

    println("historical_df长度："+historical_df.count())
    historical_df.show(false)


    val merchants_df=utils.read_csv(EloConstants.merchants)

    println("merchants长度："+merchants_df.count())
    merchants_df.show(false)



  }

  def merchantsExplor()={
    val spark=SparkUtil.getSpark()
    val utils=new DataUtils(spark)
    val merchants_df=utils.read_csv(EloConstants.merchants)

    println("merchants长度："+merchants_df.count())
    merchants_df.show(false)

//    println("去重后数量："+merchants_df.select("merchant_id").distinct().count())
    val ThanOneMerchants=merchants_df.groupBy("merchant_id").count().filter("count > 1").select("merchant_id").collect().map(_.getString(0))
    merchants_df.filter(row=>ThanOneMerchants.contains(row.getString(0))).sort("merchant_id").show(false)

  }


  def explorNewAndHistory()={

          val spark=SparkUtil.getSpark()
    val utils=new DataUtils(spark)


    val newMerChantTransactions_df=utils.read_csv(EloConstants.newMerChantTransactions_mini)



    val historical_df=utils.read_csv(EloConstants.historical_mini)
    newMerChantTransactions_df.join(historical_df,"card_id").show(false)
  }


  def exploreCardId()={
    val spark=SparkUtil.getSpark()
    val utils=new DataUtils(spark)
    val train_df=utils.read_csv(EloConstants.trainPath)


    val test_df=utils.read_csv(EloConstants.testPath)
//    val train_group=train_df.groupBy("card_id").count()
//    val train_count=train_group.filter("count > 1")
//    println(train_group.count())
//    println(train_count.count())
//    train_count.show(false)


    val joined=train_df.join(test_df,"card_id")
    println(joined.count())
    joined.show(false)
  }
}