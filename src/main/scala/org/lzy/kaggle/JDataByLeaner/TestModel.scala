package org.lzy.kaggle.JDataByLeaner

import ml.dmlc.xgboost4j.scala.spark.XGBoostEstimator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SparkSession
import org.lzy.kaggle.JDataByLeaner.Run.basePath

/**
  * Auther: lzy
  * Description:
  * Date Created by： 17:49 on 2018/6/27
  * Modified By：
  */

object TestModel {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("names3")
                //            .master("local[*]")
                .getOrCreate()
        val train = spark.read.parquet(basePath + "cache/trainMonth/02")
        val vectorAssembler = new VectorAssembler().setInputCols(train.columns).setOutputCol("assemblerFeature")
        val xgboostParam = Map(
            "booster" -> "gbtree",
            "objective" -> "reg:linear",
            "eval_metric" -> "rmse",
            "max_depth" -> 6,
            "eta" -> 0.05,
            "colsample_bytree" -> 0.9,
            "subsample" -> 0.8,
            "verbose_eval" -> 0
        )
        spark.sparkContext.setLogLevel("WARN")
        val trains=vectorAssembler.transform(train).select("label_1","assemblerFeature")
                        .toDF("labels","feature")
        trains.show()
        val xgbEstimator = new XGBoostEstimator(xgboostParam)
                    .setFeaturesCol("feature")
                .setLabelCol("labels")
                .setPredictionCol("prediction")

        println("字段名:"+xgbEstimator.getLabelCol)
//val evaluator=new UDRegressionEvaluator()
val paramGrid = new ParamGridBuilder()
        .addGrid(xgbEstimator.round, Array(10))
//            .addGrid(xgbEstimator.eta, Array(0.01,0.05))
        .addGrid(xgbEstimator.nWorkers, Array(15))
//        .addGrid(xgbEstimator.subSample,Array(0.5))
        .build()
        val tv = new TrainValidationSplit()
                .setEstimator(xgbEstimator)
//            .setEvaluator(evaluator)
                .setEvaluator(new RegressionEvaluator())
                .setEstimatorParamMaps(paramGrid)
                .setTrainRatio(0.8)

        val tvModel = tv.fit(trains)
        val predictions=tvModel.transform(trains)
        println(tvModel.getEvaluator.isLargerBetter)
        //输出评估值
        println("evaluate"+tvModel.getEvaluator.evaluate(predictions))
    }
}
