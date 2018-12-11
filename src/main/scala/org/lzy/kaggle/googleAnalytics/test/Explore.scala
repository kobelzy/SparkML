package org.lzy.kaggle.googleAnalytics.test

import common.{DataUtils, SparkUtil}
import org.lzy.kaggle.googleAnalytics.Constants

object Explore {
  def main(args: Array[String]): Unit = {
    val spark=SparkUtil.getSpark()
    import spark.implicits._
    val util=new DataUtils(spark)
    val expm1=Constants.basePath+"result/expm1.csv"
//    val data=util.readToCSV(Constants.resultPath)
    val data=util.read_csv(expm1)
    data.describe("PredictedLogRevenue").show(false)
    data.sort($"PredictedLogRevenue".desc).show(false)
  }
}
