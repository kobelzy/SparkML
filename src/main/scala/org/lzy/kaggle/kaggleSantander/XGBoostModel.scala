package org.lzy.kaggle.kaggleSantander

//import ml.dmlc.xgboost4j.scala.spark.XGBoostEstimator
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.lzy.JData.JDataByLeaner.UDLogisticEvaluator

/**
  * Auther: lzy
  * Description:
  * Date Created by： 17:50 on 2018/7/11
  * Modified By：
  */

object XGBoostModel {
    /**
      * 分裂训练集验证+逻辑回归
      * * @param train_df
      * @param labelCol
      * @param predictCol
      * @param round
      * @return
      */
    def fitPredictByLogistic(train_df:DataFrame,labelCol:String,predictCol:String,round:Int)={
        val xgboostParam=Map(
            "booster"->"gbtree",
            "objective"->"multi:softmax",
            "eval_metric"->"auc",
            "max_depth"->5,
            "eta"->0.05,
            "colsample_bytree"->0.9,
            "subsample"->0.8,
            "verbose_eval"->0
        )

        val evaluator:UDLogisticEvaluator=new UDLogisticEvaluator()

//        val xgbEstimator = new XGBoostEstimator(xgboostParam)
        //            .setPredictionCol(predictCol)

        val paramGrid = new ParamGridBuilder()
//                .addGrid(xgbEstimator.round, Array(round))
//                  .addGrid(xgbEstimator.eta, Array(0.1,0.05,0.2))
//                .addGrid(xgbEstimator.nWorkers,Array(15))
                //        .addGrid(xgbEstimator.subSample,Array(0.5))
                .build()
        val tv=new TrainValidationSplit()
//                .setEstimator(xgbEstimator)
                .setEvaluator(evaluator)
//      .setEvaluator(new MulticlassClassificationEvaluator())
                .setEstimatorParamMaps(paramGrid)
                .setTrainRatio(0.8)

        val tvModel=tv.fit(train_df.withColumnRenamed(labelCol,"label"))
        tvModel
    }


}
