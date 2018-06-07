package org.lzy.kaggle.JDataByLeaner

import java.io
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.lzy.kaggle.JDataByLeaner.Model.basePath
import scala.collection.mutable
/**
  * Created by Administrator on 2018/6/3
  spark-submit --master yarn-client --queue lzy --driver-memory 2g --conf spark.driver.maxResultSize=2g  \
--num-executors 10 --executor-cores 4 --executor-memory 7g --jars \
/root/lzy/xgboost/jvm-packages/xgboost4j-spark/target/xgboost4j-spark-0.8-SNAPSHOT-jar-with-dependencies.jar \
--class org.lzy.kaggle.JDataByLeaner.TrainModels SparkML.jar
  */
object TrainModels{
  //  val basePath = "E:\\dataset\\JData_UserShop\\"
  val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
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
    trainModel.getTrain()
  }
}
class TrainModels(spark: SparkSession, basePath: String) {
import spark.implicits._


  def score(result_df:DataFrame)={
    val udf_getWeight=udf{index:Int=>
      1/(1+math.log(index+1))
    }

    val udf_binary=udf{label_1:Int=>if (label_1>0) 1 else 0}
    val weight_df=result_df.sort("o_num").withColumn("new_label",udf_binary($"label_1")).drop("label_1").withColumnRenamed("new_label","label_1")
      .limit(50000)
          .withColumn("index",monotonically_increasing_id)
      .withColumn("weight",udf_getWeight($"index"))
    val s1=weight_df.filter($"label_1" === $"weight").select($"label_1".as[Int]).collect().sum/4674.239
    val df_label_1=weight_df.filter($"label_1" ===1)
    val s2=weight_df.select($"label_2".as[Double],$"pred_date".as[Double]).collect().map{case (label_2,pred_date)=>
    10.0/(math.round(label_2)-pred_date*pred_date+10)
    }.sum /weight_df.count()
    println(s"s1 score is $s1 ,s2 score is $s2 , S is ${0.4 * s1 + 0.6 * s2}")
  }
def getTrain()={
val train=spark.read.parquet(basePath + "cache/vali_train")
  val test=spark.read.parquet(basePath+"cache/vali_test")
  val dropColumns:Array[String]=Array("user_id","label_1","label_2")
  val featureColumns:Array[String]=train.columns.filterNot(dropColumns.contains(_))
//  val X=train.drop(dropColumns:_*)
//  val X_pred=test.drop(dropColumns:_*)
//  val y=train.select("label_1")
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

  }

  def fitPredict()={

  }
}
