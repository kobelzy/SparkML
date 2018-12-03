package org.lzy.kaggle.eloRecommendation

import common.{SparkUtils, Utils}

object DataExplore {
  def main(args: Array[String]): Unit = {
  run1()
  }


  def run1()={
    val spark=SparkUtils.getSpark("elo")
    val utils=new Utils(spark)
    val historical_df=utils.readToCSV(EloConstants.historical)
    historical_df.show(false)
    println("长度："+historical_df.count())
  }
}
