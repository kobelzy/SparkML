package org.lzy.kaggle.JDataByLeaner
import breeze.numerics.round
import ml.dmlc.xgboost4j.scala.spark.XGBoostEstimator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression._
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.DataFrame
import org.dmg.pmml.True
/**
  * Created by Administrator on 2018/6/25.
  */
object GBMmodels {

  def getS2Prediction(train:DataFrame,labelCol:String,predictCol:String,round:Int)={
    val gbmr:GBMRegressor = new GBMRegressor

    gbmr.setBoostType("gbtree")                   // "dart" -> DART, "gbtree" -> gradient boosting
      .setObjectiveFunc("square")             // "square" -> MSE, "huber" -> Pseudo-Huber loss
      .setEvaluateFunc(Array("rmse"))  // "rmse", "mse", "mae"
      .setMaxIter(10)                         // maximum number of iterations
      .setMaxDepth(7)                         // maximum depth
      .setMaxBins(32)                         // maximum number of bins
      .setNumericalBinType("width")           // "width" -> by interval-equal bins, "depth" -> by quantiles
      .setMaxLeaves(100)                      // maximum number of leaves
      .setMinNodeHess(0.001)                  // minimum hessian needed in a node
      .setRegAlpha(0.1)                       // L1 regularization
      .setRegLambda(0.5)                      // L2 regularization
      .setDropRate(0.1)                       // dropout rate
      .setDropSkip(0.5)                       // probability of skipping drop
//      .setInitialModelPath(path)              // path of initial model
      .setEarlyStopIters(10)                  // early stopping
.setEnableOneHot(true)
    //            .setPredictionCol(predictCol)
    //val evaluator=new UDRegressionEvaluator()
    val paramGrid = new ParamGridBuilder()
      .addGrid(gbmr.maxIter, Array(round))
      //        .addGrid(xgbEstimator.subSample,Array(0.5))
      .build()
    val tv=new TrainValidationSplit()
      .setEstimator(gbmr)
      //            .setEvaluator(evaluator)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)

    val tvModel=tv.fit(train
      .withColumnRenamed(labelCol,"label")
      //使用对数进行转换
      //                .withColumn("label",log(labelCol)+1).drop(labelCol)
    )

    tvModel

  }
}
