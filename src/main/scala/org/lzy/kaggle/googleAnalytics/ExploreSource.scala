package scala.org.lzy.kaggle.googleAnalytics

import common.Utils
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/10/1.
  */
object ExploreSource {
  val basePath = "E:/Dataset/GoogleAnalytics/"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("explore").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    explore(spark)
  }


  def explore(spark: SparkSession) = {
    val utils = new Utils(spark)
    //    val train=utils.readToCSV(basePath+"source/extracted_fields_train.csv")
    //    train.show(false)
    val test = utils.readToCSV(basePath + "source/extracted_fields_test.csv")
    test.show(false)
    //    val train_df=train
    //      // .select("device.browser ")
    //      .select($"totals_transactionRevenue").distinct().count()
    //    println(train_df)

    test.select("device_isMobile").distinct().show(false)
    test.select("trafficSource_isTrueDirect").distinct().show(false)

    //    test.columns.foreach(column => {
    //      println(column + ":")
    //      println(test.filter(col(column).isNull).count())
    //    })
  }
}
