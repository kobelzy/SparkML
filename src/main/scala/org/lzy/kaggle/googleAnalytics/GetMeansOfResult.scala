package org.lzy.kaggle.googleAnalytics

import common.{SparkUtil, Utils}
import org.apache.spark.sql.functions._
object GetMeansOfResult {
  def main(args: Array[String]): Unit = {
run()
  }
  def run()={
    val spark=SparkUtil.getSpark()
    import spark.implicits._
    spark.sparkContext.setLogLevel("warn")
    val util=new Utils(spark)
    val result_df=    spark.read.option("header", "true")
      .option("nullValue", "NA")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .csv(Constants.resultPath)

    result_df.show(false)
    println(result_df.count())
    val distincted=result_df.groupBy("fullVisitorId")
      .agg(mean("transactionRevenue").alias("transactionRevenue"))
      .map(raw => {
        val fullVisitorId = raw.getString(0).toString
        val transactionRevenue = raw.getDouble(1).formatted("%.4f")
        (fullVisitorId, transactionRevenue)
//        fullVisitorId,PredictedLogRevenue
      }).toDF("fullVisitorId", "PredictedLogRevenue")
    distincted.show(false)
    util.writeToCSV(distincted,Constants.basePath+"result/result.csv")
  }
}
