package org.lzy.kaggle.JDataByLeaner


import ml.dmlc.xgboost4j.scala.spark.{XGBoostEstimator, XGBoostModel}
import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressionModel
import org.apache.spark.ml
import org.apache.spark.ml.param.DoubleParam
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.ml.tuning.{TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.sql.SparkSession
import org.lzy.kaggle.JDataByLeaner.TrainModels.basePath

/**
  * Created by Administrator on 2018/6/8.
  */
object ShowModels {
  val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
            .master("local[*]")
      .appName("model").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

//    val model=TrainValidationSplitModel.read.load(basePath+s"model/s1_train_Model")
//    println(model.bestModel.explainParams())
val data03_df = spark.read.parquet(basePath + "cache/trainMonth/03")
    data03_df.sort("user_id").show(false)

//println(model.bestModel.extractParamMap())
//    val params=bestmodel.params.foreach(println)
//println(    bestmodel.getParam("eta"))
//    println(bestmodel.getParam("round"))
//    val lrModel:Transformer=bestmodel.stages(0)
//    println(lrModel.explainParams())
//    println(lrModel.explainParam(XGBoostModel.regParam))
//    println(lrModel.explainParam(XGBoost.elasticNetParam))


//    val evaluator=model.evaluator
//    println(evaluator.doc)
  }
}
