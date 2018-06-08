package org.lzy.kaggle.JDataByLeaner

import java.sql.Timestamp

import ml.dmlc.xgboost4j.scala.spark.{XGBoostEstimator, XGBoostModel}
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.TrainValidationSplitModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
/**
  * Created by Administrator on 2018/6/3
  spark-submit --master yarn-client --queue lzy --driver-memory 2g --conf spark.driver.maxResultSize=2g  \
--num-executors 12 --executor-cores 4 --executor-memory 7g --jars \
/root/lzy/xgboost/jvm-packages/xgboost4j-spark/target/xgboost4j-spark-0.8-SNAPSHOT-jar-with-dependencies.jar \
--class org.lzy.kaggle.JDataByLeaner.TrainModels SparkML.jar
  */
object TrainModels{

  //  val basePath = "E:\\dataset\\JData_UserShop\\"
  val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names2")
//            .master("local[*]")
      .getOrCreate()
    val sc=spark.sparkContext
    val config=sc.getConf
//    config.set("spark.driver.maxResultSize","0")
    config.set("spark.debug.maxToStringFields","100")
    config.set("spark.shuffle.io.maxRetries","60")
    config.set("spark.default.parallelism","54")
    spark.sparkContext.setLogLevel("WARN")
    val util = new Util(spark)
    val trainModel=new TrainModels(spark,basePath)
//    trainModel.getTrain()
//      trainModel.fitPredict()
//   val submission= trainModel.getResult().na.fill(0)
//    submission.write.mode(SaveMode.Overwrite).parquet((basePath+"sub/result_parquet"))
//    println(submission.count())
//trainModel.trainAndSaveModel("vali")

    //验证结果模型
//    trainModel.varifyTrainModel()
    //验证训练模型
    trainModel.varifyValiModel()
  }
}
class TrainModels(spark: SparkSession, basePath: String) {

  import spark.implicits._

/*
分数检验
 */
  def score(result_df:DataFrame)={
    val udf_getWeight=udf{index:Int=>
      1/(1+math.log(index))
    }

    val udf_binary=udf{label_1:Int=>if (label_1>0) 1.0 else 0.0}
    val weight_df=result_df.sort($"o_num".desc).limit(50000)
      .withColumn("label_binary",udf_binary($"label_1"))
          .withColumn("index",monotonically_increasing_id+1)
      .withColumn("weight",udf_getWeight($"index"))
    val s1=weight_df.select($"label_binary".as[Double],$"weight".as[Double]).map(tuple=>tuple._1*tuple._2).collect().sum/4674.239
    val weightEqual1_df=weight_df.filter($"label_binary" ===1)
    val s2=weightEqual1_df.select($"label_2".as[Double],$"pred_date".as[Double]).collect().map{case (label_2,pred_date)=>
    10.0/(math.pow(math.round(label_2-pred_date),2)+10)
    }.sum /weight_df.count().toDouble
    println(s"s1 score is $s1 ,s2 score is $s2 , S is ${0.4 * s1 + 0.6 * s2}")
  }



def getTrain()={
val train=spark.read.parquet(basePath + "cache/vali_train")
  val test=spark.read.parquet(basePath+"cache/vali_test")
  val dropColumns:Array[String]=Array("user_id","label_1","label_2")
  val featureColumns:Array[String]=train.columns.filterNot(dropColumns.contains(_))
  val selecter=new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")
  val train_df=selecter.transform(train)
  val test_df=selecter.transform(test)
  //为resul通过label_1来计算 添加o_num列，
val s1_Model=Model.fitPredict(train_df,test_df,"label_1","o_num")
  s1_Model.write.overwrite().save(basePath+"model/s1_Model")

  //为result通过label_2来计算添加pred_date
val s2_Model=Model.fitPredict(train_df,test_df,"label_2","pred_date")
  s2_Model.write.overwrite().save(basePath+"model/s2_Model")

  //result包括了"user_id","label_1","label_2","o_num","pred_date"

  val labelCol="label_1"
  val predictCol="o_num"
  val s1_df=s1_Model.transform(test_df.withColumnRenamed(labelCol,"label"))
              .withColumnRenamed("label",labelCol)
              .withColumnRenamed("prediction",predictCol)
              .select("user_id",labelCol,predictCol)
  val labelCol2="label_2"
  val s2_df=s1_Model.transform(test_df.withColumnRenamed(labelCol2,"label"))
          .withColumnRenamed("label",labelCol2)
          .withColumnRenamed("prediction",predictCol)
          .select("user_id",labelCol2,predictCol)

  val result=s1_df.join(s2_df,"user_id")
  score(result)
   }
  def getResult()={
    val test=spark.read.parquet(basePath+"cache/test_test")
    val dropColumns:Array[String]=Array("user_id","label_1","label_2")
    val featureColumns:Array[String]=test.columns.filterNot(dropColumns.contains(_))
    val selecter=new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")
    val test_df=selecter.transform(test)

    val s1_Model=XGBoostModel.read.load(basePath+"model/s1_Model/bestModel")
    val labelCol="label_1"
    val predictCol="o_num"
    val labelCol2="label_2"
    val predictCol2="pred_date"

    val s1_df=s1_Model.transform(test_df.withColumnRenamed(labelCol,"label"))
      .withColumnRenamed("label",labelCol)
      .withColumnRenamed("prediction",predictCol)
      .select("user_id",labelCol,predictCol)
    val s2_df=s1_Model.transform(test_df.withColumnRenamed(labelCol2,"label"))
      .withColumnRenamed("label",labelCol2)
      .withColumnRenamed("prediction",predictCol2)
      .select("user_id",labelCol2,predictCol2)

    val result=s1_df.join(s2_df,"user_id")
    score(result)
    val udfs=udf{(pred_date:Double)=>{
      val days=1+math.round(pred_date+0.49-1)
      getTime(s"2017-05-${days}")
    }}
    val submission:DataFrame=result.sort($"o_num")
//            .withColumn("result_date",udfs($"pred_date"))
//      .select("user_id","result_date")
//      .map(tuple=>class_result(tuple.getString(0),tuple.getString(1)))
    submission.show(20,false)
//submission.coalesce(1).write
//  .option("header", "true")
//    .mode(SaveMode.Overwrite)
//  .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
//      .option("nullValue", "NA")
//  .csv(basePath+"sub/result")
//
//    submission.write.mode(SaveMode.Overwrite).parquet((basePath+"sub/result_parquet"))
//submission.rdd.coalesce(1).saveAsTextFile(basePath+"sub/result")
    submission
  }
  def getTime(yyyy_MM_dd: String) = {
    Timestamp.valueOf(yyyy_MM_dd + " 00:00:00")
  }

  /**
    * 训练并保存数据
    * dataType为test或者vali
    */
  def trainAndSaveModel(dataType:String="test")={
      val train=spark.read.parquet(basePath + s"cache/${dataType}_test")
      val test=spark.read.parquet(basePath+s"cache/${dataType}_test")
      val dropColumns:Array[String]=Array("user_id","label_1","label_2")
      val featureColumns:Array[String]=train.columns.filterNot(dropColumns.contains(_))
      val selecter=new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")
      val train_df=selecter.transform(train)
      val test_df=selecter.transform(test)
      //为resul通过label_1来计算 添加o_num列，
      val s1_Model:TrainValidationSplitModel=Model.fitPredict(train_df,test_df,"label_1","o_num")
      s1_Model.write.overwrite().save(basePath+s"model/s1_${dataType}_Model")

      //为result通过label_2来计算添加pred_date
      val s2_Model=Model.fitPredict(train_df,test_df,"label_2","pred_date")
      s2_Model.write.overwrite().save(basePath+s"model/s2_${dataType}_Model")
  }



  /**
    * 检验模型准确性
    */
  def varifyTrainModel()={
    val test=spark.read.parquet(basePath+s"cache/test_test")
    val dropColumns:Array[String]=Array("user_id","label_1","label_2")
    val featureColumns:Array[String]=test.columns.filterNot(dropColumns.contains(_))
    val selecter=new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")
    val test_df=selecter.transform(test)

    val s1_Model=XGBoostModel.read.load(basePath+s"model/s1_Model/bestModel")
    val s2_Model=XGBoostModel.read.load(basePath+s"model/s2_Model/bestModel")
    val labelCol="label_1"
    val predictCol="o_num"
    val labelCol2="label_2"
    val predictCol2="pred_date"

    val s1_df=s1_Model.transform(test_df.withColumnRenamed(labelCol,"label"))
      .withColumnRenamed("label",labelCol)
      .withColumnRenamed("prediction",predictCol)
      .select("user_id",labelCol,predictCol)
    val s2_df=s2_Model.transform(test_df.withColumnRenamed(labelCol2,"label"))
      .withColumnRenamed("label",labelCol2)
      .withColumnRenamed("prediction",predictCol2)
      .select("user_id",labelCol2,predictCol2)

    val result=s1_df.join(s2_df,"user_id")
    score(result)
  }

  /**
    * 检验模型准确性
    */
  def varifyValiModel()={
    val test=spark.read.parquet(basePath+s"cache/vali_test")
    val dropColumns:Array[String]=Array("user_id","label_1","label_2")
    val featureColumns:Array[String]=test.columns.filterNot(dropColumns.contains(_))
    val selecter=new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")
    val test_df=selecter.transform(test)

    val s1_Model=XGBoostModel.read.load(basePath+s"model/s1_train_Model/bestModel")
    val s2_Model=XGBoostModel.read.load(basePath+s"model/s2_train_Model/bestModel")
    val labelCol="label_1"
    val predictCol="o_num"
    val labelCol2="label_2"
    val predictCol2="pred_date"

    val s1_df=s1_Model.transform(test_df.withColumnRenamed(labelCol,"label"))
      .withColumnRenamed("label",labelCol)
      .withColumnRenamed("prediction",predictCol)
      .select("user_id",labelCol,predictCol)
    val s2_df=s2_Model.transform(test_df.withColumnRenamed(labelCol2,"label"))
      .withColumnRenamed("label",labelCol2)
      .withColumnRenamed("prediction",predictCol2)
      .select("user_id",labelCol2,predictCol2)

    val result=s1_df.join(s2_df,"user_id")
    score(result)
  }

  def showModelParams()={
    val s1_Model=XGBoostModel.read.load(basePath+s"model/s1_train_Model/bestModel")
//    val bestmodel = s1_Model.asInstanceOf[PipelineModel]
//    val lrModel:Transformer=bestmodel.stages(2)
//    println(lrModel.explainParam(XGBoostModel.regParam))
//    println(lrModel.explainParam(XGBoost.elasticNetParam))
  }
}
