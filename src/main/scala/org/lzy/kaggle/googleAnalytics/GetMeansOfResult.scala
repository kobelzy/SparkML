package org.lzy.kaggle.googleAnalytics

import common.{DataUtils, SparkUtil}
import org.apache.spark.sql.functions._
object GetMeansOfResult {
  def main(args: Array[String]): Unit = {
run()
  }
  def run()={
    val spark=SparkUtil.getSpark()
    import spark.implicits._
    spark.sparkContext.setLogLevel("warn")
    val util=new DataUtils(spark)
    val result_df=    spark.read.option("header", "true")
      .option("nullValue", "NA")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .csv(Constants.resultPath)

    result_df.show(false)
    println(result_df.count())
    val distincted=result_df
      .map(raw => {
        val fullVisitorId = raw.getString(0).toString
        val transactionRevenue = math.expm1(raw.getString(1).toDouble) match {
          case a if a>=1000000  =>1000000
          case b =>b
        }
        (fullVisitorId, transactionRevenue)
//        fullVisitorId,PredictedLogRevenue
      }).toDF("fullVisitorId", "PredictedLogRevenue")
    distincted.show(false)
    util.to_csv(distincted,Constants.basePath+"result/expm1.csv")
  }

  def run2()={
    val d1=0.008112021281382269
    println(math.expm1(d1))
  }
}
