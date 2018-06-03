package org.lzy.kaggle.JDataByLeaner

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/6/3.
  */
object TrainModels{
  //  val basePath = "E:\\dataset\\JData_UserShop\\"
  val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
            .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val util = new Util(spark)
    val trainModel=new TrainModels(spark,basePath)
  }
}
class TrainModels(spark: SparkSession, basePath: String) {
import spark.implicits._
}
