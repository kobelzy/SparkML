package org.lzy.kaggle.JDataByLeaner

import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostEstimator, XGBoostModel}
import org.apache.spark.ml
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Administrator on 2018/6/5.
  */
object Model {
  val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
  case class datas(features:ml.linalg.Vector, label:Double)
  def main(args: Array[String]): Unit = {
    val Array(round,nWorkers)=args
    val spark=SparkSession.builder()
//      .master("local[*]")
      .appName("model").getOrCreate()
    val config=spark.sparkContext.getConf
    config.set("spark.debug.maxToStringFields","100")
    config.set("spark.shuffle.io.maxRetries","60")
    config.set("spark.default.parallelism","54")
import spark.implicits._
val data:DataFrame =MLUtils.loadLibSVMFile(spark.sparkContext,basePath+"linear_regression_data.txt")
        .map(line=>datas(line.features.asML,line.label)).toDF("features","labels")
    data.show(false)
    val Array(train,test)=data.randomSplit(Array(0.8,0.2))
    val result =featModel(train,test,round.toInt,nWorkers.toInt)
    result .show(false)
  }


def featModel(train_df:DataFrame,test_df:DataFrame,round:Int,nWorkers:Int)={
  //迭代次数
//  val round=50
//  //
//  val nWorkers=10
  val useExternalMemory=true
    val xgboostParam=Map(
        "booster"->"gbtree",
        "objection"->"reg:linear",
        "eval_metric"->"rmse",
        "max_depth"->4,
      "eta"->0.05,
        "colsample_bytree"->0.9,
        "subsample"->0.8,
        "verbose_eval"->0
    )
    val xgbEstimator = new XGBoostEstimator(xgboostParam).setLabelCol("labels")
//            .setPredictionCol("result")

    val paramGrid = new ParamGridBuilder()
            .addGrid(xgbEstimator.eta, Array(0.05))
            .build()

    val tv=new TrainValidationSplit()
            .setEstimator(xgbEstimator)
            .setEvaluator(new RegressionEvaluator())
            .setEstimatorParamMaps(paramGrid)
            .setTrainRatio(0.8)
    val tvModel=tv.fit(train_df)
    val result_df=tvModel.transform(test_df).withColumnRenamed("prediction","result")
    result_df
}

  def fitPredict(train_df:DataFrame,labelCol:String,predictCol:String)={
val useExternalMemory=true
    val xgboostParam=Map(
      "booster"->"gbtree",
      "objection"->"reg:linear",
      "eval_metric"->"rmse",
      "max_depth"->5,
      "eta"->0.05,
      "colsample_bytree"->0.9,
      "subsample"->0.8,
      "verbose_eval"->0
    )

    val xgbEstimator = new XGBoostEstimator(xgboostParam)
//            .setPredictionCol(predictCol)
    val paramGrid = new ParamGridBuilder()
            .addGrid(xgbEstimator.round, Array(2000))
//            .addGrid(xgbEstimator.eta, Array(0.01,0.05))
        .addGrid(xgbEstimator.nWorkers,Array(12))
//        .addGrid(xgbEstimator.subSample,Array(0.5))
            .build()
    val tv=new TrainValidationSplit()
            .setEstimator(xgbEstimator)
            .setEvaluator(new RegressionEvaluator())
            .setEstimatorParamMaps(paramGrid)
            .setTrainRatio(0.8)
    val tvModel=tv.fit(train_df.withColumnRenamed(labelCol,"label"))
//      val result_df=tvModel.transform(test_df.withColumnRenamed(labelCol,"label"))
//              .withColumnRenamed("label",labelCol)
//              .withColumnRenamed("prediction",predictCol)
//              .select("user_id",labelCol,predictCol)
//      val bestmodel = tvModel.bestModel.asInstanceOf[PipelineModel]
//      val lrModel:Transformer=bestmodel.stages(0)
//      println("round:"+lrModel.explainParam(xgbEstimator.round))
//      println("eta:"+lrModel.explainParam(xgbEstimator.eta))
      tvModel
  }
}
