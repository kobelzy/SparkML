package org.lzy.kaggle.kaggleSantander

import common.Utils
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/7/14.
  */
object ShowFeatures {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
                  .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val conf = spark.conf
    val sc = spark.sparkContext
    val config = sc.getConf
    //    config.set("spark.driver.maxResultSize","0")
    config.set("spark.debug.maxToStringFields", "100")
    config.set("spark.shuffle.io.maxRetries", "60")
    config.set("spark.default.parallelism", "54")
    config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val utils = new Utils(spark)
    val models = new Models(spark)
    val featureExact = new FeatureExact(spark)
    val run = new Run(spark)
    val trainModel = new TrainModel(spark)
    val train_df = utils.readToCSV(Constant.basePath + "AData/train.csv").repartition(100).cache()
//    val test_df = utils.readToCSV(Constant.basePath + "AData/test.csv").repartition(100).cache()
    val features_df=train_df.select("target","f190486d6")
    features_df.show(1000,false)

  }
}
