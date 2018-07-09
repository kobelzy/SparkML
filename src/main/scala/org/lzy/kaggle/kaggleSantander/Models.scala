package org.lzy.kaggle.kaggleSantander

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Administrator on 2018/7/3.
  */
class Models(spark:SparkSession) {
import spark.implicits._




  def LR_TranAndSave(data:DataFrame,label:String="label",features:String="features")={
    val lr=new LinearRegression()
      .setMaxIter(1000)
      .setLabelCol(label)
      .setFeaturesCol(features)

    val lr_model=lr.fit(data)
    lr_model.write.overwrite().save(Constant.basePath+"model/lr_model")
    lr_model
  }

  def RF_TrainAndSave(data:DataFrame,label:String="label",features:String="features")={
    val rf=new RandomForestRegressor()
            .setLabelCol(label).setFeaturesCol(features)

    val rf_model=rf.fit(data)

    rf_model.write.overwrite().save(Constant.basePath+"model/rf_model")
    rf_model
  }

  def GBDT_TrainAndSave(data:DataFrame,label:String="label",features:String="features")={
    val gbdt=new GBTRegressor()
            .setLabelCol(label).setFeaturesCol(features)
            .setMaxIter(100)
    val gbdt_model=gbdt.fit(data)

    gbdt_model.write.overwrite().save(Constant.basePath+"model/gbdt_model")
    gbdt_model
  }

  def trainSplit_TrainAndSave(data:DataFrame,label:String="label",features:String="features")={
    val gbdt=new GBTRegressor()
      .setLabelCol(label).setFeaturesCol(features)
    val paramGrid=new ParamGridBuilder()
        .addGrid(gbdt.maxIter,Array(100))
      .build()
    val evaluator=new ScoreEvaluator()
    val tv=new TrainValidationSplit()
      .setEstimator(gbdt)
.setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
          .setTrainRatio(0.8)
  }

  def evaluateGDBT(data:DataFrame,label:String="label",features:String="features")={
    val Array(train,test)=data.randomSplit(Array(0.8,0.2),seed=10)
    val gbdt=new GBTRegressor()
      .setLabelCol(label).setFeaturesCol(features)
      .setMaxIter(100)
    val gbdt_model=gbdt.fit(train)
    val prediction=gbdt_model.transform(test)

//    val evaluator=new ScoreEvaluator()
//      evaluator.setLabelCol(label)
    val lr_evaluator=new RegressionEvaluator().setLabelCol(label).setMetricName("rmse")
    val score=lr_evaluator.evaluate(prediction)

      score
  }
}
