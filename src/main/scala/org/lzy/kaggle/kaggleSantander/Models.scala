package org.lzy.kaggle.kaggleSantander

import org.apache.spark.ml.classification.{GBTClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Administrator on 2018/7/3.
  */
class Models(spark: SparkSession) {

    import spark.implicits._


    /** *
      * 功能实现:
      * 使用LR来训练模型，并保存模型，返回该模型
      * Author: Lzy
      * Date: 2018/7/9 9:15
      * Param: [data, label, features]
      * Return: org.apache.spark.ml.regression.LinearRegressionModel
      */
    def LR_TranAndSave(data: DataFrame, label: String = "label", features: String = "features") = {
        val lr = new LinearRegression()
                .setMaxIter(1000)
                .setLabelCol(label)
                .setFeaturesCol(features)

        val lr_model = lr.fit(data)
        lr_model.write.overwrite().save(Constant.basePath + "model/lr_model")
        lr_model
    }

    /** *
      * 功能实现:
      * 使用随机森林来训练模型，并保存模型，返回该模型
      * Author: Lzy
      * Date: 2018/7/9 9:16
      * Param: [data, label, features]
      * Return: org.apache.spark.ml.regression.RandomForestRegressionModel
      */
    def RF_TrainAndSave(data: DataFrame, label: String = "label", features: String = "features") = {
        val rf = new RandomForestRegressor()
                .setLabelCol(label).setFeaturesCol(features)

        val rf_model = rf.fit(data)

        rf_model.write.overwrite().save(Constant.basePath + "model/rf_model")
        rf_model
    }

    /** *
      * 功能实现:
      * 使用GBDT来训练模型，并保存模型和返回
      * Author: Lzy
      * Date: 2018/7/9 9:16
      * Param: [data, label, features]
      * Return: org.apache.spark.ml.regression.GBTRegressionModel
      */
    def GBDT_TrainAndSave(data: DataFrame, label: String = "label", features: String = "features") = {
        val gbdt = new GBTRegressor()
                .setLabelCol(label).setFeaturesCol(features)
                .setMaxIter(100)
        val gbdt_model = gbdt.fit(data)

        gbdt_model.write.overwrite().save(Constant.basePath + "model/gbdt_model")
        gbdt_model
    }

    def GBDTClassic_TrianAndSave(data: DataFrame, label: String = "label", features: String = "features") = {
        val gbdt = new GBTClassifier()
                .setLabelCol(label).setFeaturesCol(features)

                .setMaxIter(100)
        val gbdt_model = gbdt.fit(data)

        gbdt_model.write.overwrite().save(Constant.basePath + "model/gbdt_classic_model")
        gbdt_model
    }

    def RFClassic_TrainAndSave(data: DataFrame, label: String = "label", features: String = "features") = {
        val gbdt = new RandomForestClassifier()
                .setLabelCol(label).setFeaturesCol(features)
                .setSeed(10)
                .setNumTrees(500)

        val gbdt_model = gbdt.fit(data)

        gbdt_model.write.overwrite().save(Constant.basePath + "model/rf_classic_model")
        gbdt_model
    }

    def trainSplit_TrainAndSave(data: DataFrame, label: String = "label", features: String = "features") = {
        val gbdt = new GBTRegressor()
                .setLabelCol(label).setFeaturesCol(features)
        val paramGrid = new ParamGridBuilder()
                .addGrid(gbdt.maxIter, Array(100))
                .build()
        val evaluator = new ScoreEvaluator()
        val tv = new TrainValidationSplit()
                .setEstimator(gbdt)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramGrid)
                .setTrainRatio(0.8)
    }

    /** *
      * 功能实现:
      * 使用GBDT训练模型，使用分裂验证，返回其分数值
      * Author: Lzy
      * Date: 2018/7/9 9:17
      * Param: [data, label, features]
      * Return: double
      */
    def evaluateGDBT(data: DataFrame, label: String = "label", features: String = "features") = {
        val Array(train, test) = data.randomSplit(Array(0.8, 0.2), seed = 10)
        val gbdt = new GBTRegressor()
                .setLabelCol(label).setFeaturesCol(features)
                .setMaxIter(100)
        val gbdt_model = gbdt.fit(train)
        val prediction = gbdt_model.transform(test)
        //    val evaluator=new ScoreEvaluator()
//      evaluator.setLabelCol(label)
val lr_evaluator = new RegressionEvaluator().setLabelCol(label).setMetricName("rmse")
        val score = lr_evaluator.evaluate(prediction)
        score
    }

}
