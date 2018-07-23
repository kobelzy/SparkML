package org.lzy.JData.JDataByLeaner

import org.apache
import org.apache.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

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
生成基本数据
 */
//    runLoadData(spark)

    /*
    处理特征数据
     */
//            getFeatureData(spark)


    /*
    训练测试模型
     */

//            trainValiModel(spark,100,200)
    //        //验证训练模型
//            varifyValiModel(spark)

    /*    Range(100,300,20).map(num=>{
            println("特征数量："+num)
            trainValiModel(spark,100,num)
            varifyValiModel(spark)
        })*/

    /*
    训练结果模型并导出
     */
//            trainTestModel(spark,1000,200)

    trainTestModelByBagging(spark,1000)
    bagging(spark)


//    val trainModel = new TrainModels(spark, basePath)
//    val data04_df = spark.read.parquet(basePath + "cache/trainMonth/04")
//    trainModel.getResult("test", data04_df)

  }
  /*
生成基本数据
*/
  def runLoadData(spark:SparkSession): Unit ={
    val util = new Util(spark)
    val (order, action) = util.loadData(basePath+"data/")
    order.write.mode(SaveMode.Overwrite).parquet(basePath + "cache/order")
    action.write.mode(SaveMode.Overwrite).parquet(basePath + "cache/action")
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
    val order_cache =spark.read.parquet(basePath + "cache/order").repartition(60).cache()
    val action_cache = spark.read.parquet(basePath + "cache/action").repartition(60).cache()
    val user = util.getSourceData(basePath + "jdata_user_basic_info.csv").repartition(60).cache()

//    feaExact.save11To4MonthData(order_cache, action_cache, user)
    feaExact.save2To8MonthData(order_cache, action_cache, user)

    println("特征处理完毕")
  }

  /**
    * 训练结果模型，测试，并将数据导出
    *
    * @param spark
    */
  def trainTestModel(spark: SparkSession, round: Int = 1000, topNumFeatures: Int): Unit = {
    val trainModel = new TrainModels(spark, basePath)
//    val data04_df = spark.read.parquet(basePath + "cache/trainMonth/04")
//    val data03_df = spark.read.parquet(basePath + "cache/trainMonth/03")
//    val data02_df = spark.read.parquet(basePath + "cache/trainMonth/02")
//    val data01_df = spark.read.parquet(basePath + "cache/trainMonth/01")
//    val data12_df = spark.read.parquet(basePath + "cache/trainMonth/12")
//    val data11_df = spark.read.parquet(basePath + "cache/trainMonth/11")
//    val data10_df = spark.read.parquet(basePath + "cache/trainMonth/10")

    val data08_df = spark.read.parquet(basePath + "cache/trainMonth/08")
    val data07_df = spark.read.parquet(basePath + "cache/trainMonth/07")
    val data06_df = spark.read.parquet(basePath + "cache/trainMonth/06")
    val data05_df = spark.read.parquet(basePath + "cache/trainMonth/05")
    val data04_df = spark.read.parquet(basePath + "cache/trainMonth/04")
    val data03_df = spark.read.parquet(basePath + "cache/trainMonth/03")
    val data02_df = spark.read.parquet(basePath + "cache/trainMonth/02")


    //结果模型
    val testTrain_df = data02_df.union(data03_df).union(data04_df).union(data05_df).union(data06_df).union(data07_df).repartition(200).cache()
    trainModel.trainAndSaveModel("test", testTrain_df, round, topNumFeatures)
    trainModel.varifyModel("test", data04_df)
    trainModel.getResult("test", data04_df)
  }
  /**
    * 训练结果模型，测试，并将数据导出
    *
    * @param spark
    */
  def trainTestModelByBagging(spark: SparkSession, round: Int = 1000): Unit = {
    import spark.implicits._
    val trainModel = new TrainModels(spark, basePath)
//    val data04_df = spark.read.parquet(basePath + "cache/trainMonth/04")
//    val data03_df = spark.read.parquet(basePath + "cache/trainMonth/03")
//    val data02_df = spark.read.parquet(basePath + "cache/trainMonth/02")
//    val data01_df = spark.read.parquet(basePath + "cache/trainMonth/01")
//    val data12_df = spark.read.parquet(basePath + "cache/trainMonth/12")
//    val data11_df = spark.read.parquet(basePath + "cache/trainMonth/11")
//    val data10_df = spark.read.parquet(basePath + "cache/trainMonth/10")

    val data08_df = spark.read.parquet(basePath + "cache/trainMonth/08")
        val data07_df = spark.read.parquet(basePath + "cache/trainMonth/07")
    val data06_df = spark.read.parquet(basePath + "cache/trainMonth/06")
    val data05_df = spark.read.parquet(basePath + "cache/trainMonth/05")
    val data04_df = spark.read.parquet(basePath + "cache/trainMonth/04")
    val data03_df = spark.read.parquet(basePath + "cache/trainMonth/03")
    val data02_df = spark.read.parquet(basePath + "cache/trainMonth/02")


    //结果模型
    val testTrain_df = data02_df.union(data03_df).union(data04_df).union(data05_df).union(data06_df).union(data07_df).repartition(200).cache()
        Range(200,350,50).map(num=>{
//        Array(1250,1500).map(rounds=>{
        println("特征数量："+num)
    trainModel.trainAndSaveModel("test", testTrain_df, round, 200)
    val news=trainModel.getSDF("test", data08_df)
//            .select($"user_id",$"o_num".as("new_num"),$"pred_date".as("new_pred_date"))
//          df.join(news,"user_id").select("user_id","o_num","pred_date","new_num","new_pred_date")
//                  .map{case(Row(user_id:Int,o_num:Double,pred_date:Double,new_num:Double,new_pred_date:Double))=>
//                  }
          news.write.mode(SaveMode.Overwrite).parquet(basePath+s"sub/${num}")
    })


  }

  def bagging(spark:SparkSession)={
    import spark.implicits._
    val df_200=spark.read.parquet(basePath + "sub/200").select($"user_id",$"o_num".as("o_num_200"),$"pred_date".as("pred_date_200"))
    val df_250=spark.read.parquet(basePath + "sub/250").select($"user_id",$"o_num".as("o_num_250"),$"pred_date".as("pred_date_250"))
    val df_300=spark.read.parquet(basePath + "sub/300").select($"user_id",$"o_num".as("o_num_300"),$"pred_date".as("pred_date_300"))
    val all_df=df_200.join(broadcast(df_250),"user_id").join(broadcast(df_300),"user_id")
    val result=all_df.withColumn("o_num",($"o_num_200"+$"o_num_250"+$"o_num_300")/3.0)
      .withColumn("pred_date",($"pred_date_200"+$"pred_date_250"+$"pred_date_300")/3.0)
    val udf_predDateToDate = udf { (pred_date: Double) => s"2017-09-${math.round(pred_date)}" }
    val submission: DataFrame = result
      //      .filter($"pred_date"> -30.5 && $"pred_date" <0)
      .sort($"o_num".desc).limit(50000)
      .withColumn("result_date", udf_predDateToDate($"pred_date"))
      .select($"user_id", to_date($"result_date").as("pred_date"))
    submission.show(20, false)
    val errorData = submission.filter($"pred_date".isNull)
    errorData.join(result, "user_id").show(false)
    println("数据不合法" + errorData.count())
    println("结果数量：" + submission.count())
    println("最大：")
    println(submission.sort($"pred_date".desc).head().getDate(1))
    submission.coalesce(1).write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      //          .option("nullValue", "NA")
      .csv(basePath + "sub/result")
  }

  /** *
    * 功能实现:
    * 选联测试模型
    * Author: Lzy
    * Date: 2018/6/12 16:33
    * Param: [spark]
    * Return: void
    */
  def trainValiModel(spark: SparkSession, round: Int = 100, topNumFeatures: Int) = {
    import spark.implicits._
    val trainModel = new TrainModels(spark, basePath)
//    val data02_df = spark.read.parquet(basePath + "cache/trainMonth/02")
//    val data01_df = spark.read.parquet(basePath + "cache/trainMonth/01")
//    val data12_df = spark.read.parquet(basePath + "cache/trainMonth/12")
//    val data11_df = spark.read.parquet(basePath + "cache/trainMonth/11")
//    val data10_df = spark.read.parquet(basePath + "cache/trainMonth/10")
//    val data09_df = spark.read.parquet(basePath + "cache/trainMonth/09")

//val data02_df = spark.read.parquet(basePath + "cache/trainMonth/08")
//    val data01_df = spark.read.parquet(basePath + "cache/trainMonth/07")
    val data06_df = spark.read.parquet(basePath + "cache/trainMonth/06")
    val data05_df = spark.read.parquet(basePath + "cache/trainMonth/05")
val data04_df = spark.read.parquet(basePath + "cache/trainMonth/04")
    val data03_df = spark.read.parquet(basePath + "cache/trainMonth/03")
    val data02_df = spark.read.parquet(basePath + "cache/trainMonth/02")
    val data01_df = spark.read.parquet(basePath + "cache/trainMonth/01")
    //验证模型
    val valiTrain_df = data01_df.union(data02_df).union(data03_df).union(data04_df).union(data05_df).union(data06_df)
//            .withColumn("label_1",when($"label_1" >=1,1).otherwise(0))
            .repartition(200).cache()

    trainModel.trainAndSaveModel("vali", valiTrain_df, round, topNumFeatures)
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
    import spark.implicits._
    val trainModel = new TrainModels(spark, basePath)
    val data07_df = spark.read.parquet(basePath + "cache/trainMonth/07")
//            .withColumn("label_1",when($"label_1" >=1,1).otherwise(0))
            .cache()
    val valiTest_df = data07_df
    trainModel.varifyModel("vali", valiTest_df)
    //        trainModel.varifyValiModel("vali", valiTest_df)

  }

  def getData(spark: SparkSession,start:Int,end:Int)={
    val data02_df = spark.read.parquet(basePath + "cache/trainMonth/02")
    val data01_df = spark.read.parquet(basePath + "cache/trainMonth/01")
    val data12_df = spark.read.parquet(basePath + "cache/trainMonth/12")
    val data11_df = spark.read.parquet(basePath + "cache/trainMonth/11")
    val data10_df = spark.read.parquet(basePath + "cache/trainMonth/10")
    val data09_df = spark.read.parquet(basePath + "cache/trainMonth/09")

    Range(start,end,1).map(date=>{

    })
  }
}
