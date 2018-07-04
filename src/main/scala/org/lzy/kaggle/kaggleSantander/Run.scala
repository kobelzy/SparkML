package org.lzy.kaggle.kaggleSantander

import common.{FeatureUtils, Utils}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
  * Created by Administrator on 2018/7/3.

* Created by Administrator on 2018/6/3
 spark-submit --master yarn-client --queue lzy --driver-memory 6g --conf spark.driver.maxResultSize=5g  \
 --num-executors 12 --executor-cores 4 --executor-memory 7g  \
 --class org.lzy.kaggle.kaggleSantander.Run SparkML.jar
  * */
object Run{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
//            .master("local[*]")
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
    val utils=new Utils(spark)
    val models=new Models(spark)
    val train_df=utils.readToCSV(Constant.basePath+"AData/train.csv").repartition(100).cache()
    val test_df=utils.readToCSV(Constant.basePath+"AData/test.csv").repartition(100).cache()

    val featureFilterColumns_arr=Array("id","target")
    val featureColumns_arr=train_df.columns.filterNot(column=>featureFilterColumns_arr.contains(column.toLowerCase))
    var stages:Array[PipelineStage]=FeatureUtils.vectorAssemble(featureColumns_arr,"assmbleFeatures")
     stages=stages:+  FeatureUtils.chiSqSelector("target","assmbleFeatures","features",1000)
    val pipeline=new Pipeline().setStages(stages)

    val pipelin_model=pipeline.fit(train_df)
    val train_willFit_df=pipelin_model.transform(train_df).select("ID","target","features").withColumn("target",$"target"/10000d)
    val test_willFit_df=pipelin_model.transform(test_df).select("id","features")

    val lr_model=models.LR_TranAndSave(train_willFit_df,"target")
    val format_udf=udf{prediction:Double=>
      "%08.9f".format(prediction)
    }
    val result_df=lr_model.transform(test_willFit_df).withColumn("target",format_udf(abs($"prediction"*10000d)))
      .select("id","target")
    utils.writeToCSV(result_df,Constant.basePath+s"submission/lr_${System.currentTimeMillis()}")
  }
  def run(): Unit ={

  }
}
class Run {

}
