package org.lzy.kaggle.googleAnalytics.test

import common.{SparkUtil, Utils}
import org.lzy.kaggle.googleAnalytics.Constants

object Explore {
  def main(args: Array[String]): Unit = {
    val spark=SparkUtil.getSpark()
    val util=new Utils(spark)
    val data=util.readToCSV(Constants.trainPath)
    data.select("totals_transactionRevenue").describe("totals_transactionRevenue").show(false)
  }
}
