package org.lzy.kaggle.JDataByLeaner

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Auther: lzy
  * Description:
  * Date Created by： 9:29 on 2018/6/8
  * Modified By：
  */

object Run {
    val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("run")
                .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        val sc = spark.sparkContext
        val config = sc.getConf
//    config.set("spark.driver.maxResultSize","0")
        config.set("spark.debug.maxToStringFields", "100")
        config.set("spark.shuffle.io.maxRetries", "60")
        config.set("spark.default.parallelism", "100")
        config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        /*
        处理特征数据
         */
        getFeatureData(spark)



        /*
        训练测试模型
         */

//        trainValiModel(spark,100,200)
////        //验证训练模型
//        varifyValiModel(spark)

    Range(100,300,20).map(num=>{
        println("特征数量："+num)
        trainValiModel(spark,100,num)
        varifyValiModel(spark)
    })

        /*
        训练结果模型并导出
         */
//        trainTestModel(spark,1000)
    }

    /** *
      * 功能实现:
      * 处理特征数据
      * Author: Lzy
      * Date: 2018/6/12 16:27
      * Param: [spark]
      * Return: void
      */
    def getFeatureData(spark: SparkSession): Unit = {
        val feaExact = new FeaExact(spark, basePath)
        val util = new Util(spark)
        val order_cache = spark.read.parquet(basePath + "cache/order").repartition(60).cache()
        val action_cache = spark.read.parquet(basePath + "cache/action").repartition(60).cache()
        val user = util.getSourceData(basePath + "jdata_user_basic_info.csv").repartition(60).cache()

        feaExact.save11To4MonthData(order_cache, action_cache, user)
        println("特征处理完毕")
    }

    /**
      * 训练结果模型，测试，并将数据导出
      *
      * @param spark
      */
    def trainTestModel(spark: SparkSession,round:Int=1000,topNumFeatures:Int): Unit = {
        val trainModel = new TrainModels(spark, basePath)
        val data04_df = spark.read.parquet(basePath + "cache/trainMonth/04")
        val data03_df = spark.read.parquet(basePath + "cache/trainMonth/03")
        val data02_df = spark.read.parquet(basePath + "cache/trainMonth/02")
        val data01_df = spark.read.parquet(basePath + "cache/trainMonth/01")
        val data12_df = spark.read.parquet(basePath + "cache/trainMonth/12")
        val data11_df = spark.read.parquet(basePath + "cache/trainMonth/11")
        val data10_df = spark.read.parquet(basePath + "cache/trainMonth/10")
        //结果模型
        val testTrain_df = data10_df.union(data11_df).union(data12_df).union(data01_df).union(data02_df).union(data03_df).repartition(200)
        val testTest_df = data04_df
        trainModel.trainAndSaveModel("test", testTrain_df, round,topNumFeatures)
        trainModel.varifyModel("test", testTest_df)
        trainModel.getResult("test", testTest_df)
    }

    /** *
      * 功能实现:
      * 选联测试模型
      * Author: Lzy
      * Date: 2018/6/12 16:33
      * Param: [spark]
      * Return: void
      */
    def trainValiModel(spark: SparkSession,round:Int=100,topNumFeatures:Int) = {
        val trainModel = new TrainModels(spark, basePath)
        val data02_df = spark.read.parquet(basePath + "cache/trainMonth/02")
        val data01_df = spark.read.parquet(basePath + "cache/trainMonth/01")
        val data12_df = spark.read.parquet(basePath + "cache/trainMonth/12")
        val data11_df = spark.read.parquet(basePath + "cache/trainMonth/11")
        val data10_df = spark.read.parquet(basePath + "cache/trainMonth/10")
        val data09_df = spark.read.parquet(basePath + "cache/trainMonth/09")

        //验证模型
        val valiTrain_df = data09_df.union(data10_df).union(data11_df).union(data12_df).union(data01_df).union(data02_df).repartition(200)
        trainModel.trainAndSaveModel("vali", valiTrain_df, round,topNumFeatures)
//        trainModel.trainValiModel("vali", valiTrain_df, round)
    }

    /** *
      * 功能实现:
      * 验证测试模型
      * Author: Lzy
      * Date: 2018/6/12 16:32
      * Param: [spark]
      * Return: void
      */
    def varifyValiModel(spark: SparkSession) = {
        val trainModel = new TrainModels(spark, basePath)
        val data03_df = spark.read.parquet(basePath + "cache/trainMonth/03")
        val valiTest_df = data03_df
        trainModel.varifyModel("vali", valiTest_df)
//        trainModel.varifyValiModel("vali", valiTest_df)

    }
}
