package common

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Auther: lzy
  * Description:
  * Date Created by： 17:44 on 2018/6/27
  * Modified By：
  */

class ModelUtils(spark: SparkSession) {

    import spark.implicits._

    /**
      * XGBoost相关模型
      */
    /**
      * 分裂训练集验证+线性label1
      *
      * @param train_df
      * @param labelCol
      * @param predictCol
      * @param round
      * @return
      */
    def fitPredict(train_df: DataFrame, labelCol: String, predictCol: String, round: Int, objection: String = "reg:linear") = {
        val evalMetric: String = objection match {
            case "reg:linear" => "rmse"
            case "reg:logistic" => "auc"
        }
        val xgboostParam = Map(
            "booster" -> "gbtree",
            "objective" -> objection,
            "eval_metric" -> evalMetric,
            "max_depth" -> 6,
            "eta" -> 0.05,
            "colsample_bytree" -> 0.9,
            "subsample" -> 0.8,
            "verbose_eval" -> 0
        )
//        val xgbEstimator = new XGBoostEstimator(xgboostParam)
        //            .setPredictionCol(predictCol)
//val evaluator=new UDRegressionEvaluator()
val paramGrid = new ParamGridBuilder()
//        .addGrid(xgbEstimator.round, Array(round))
//            .addGrid(xgbEstimator.eta, Array(0.01,0.05))
//        .addGrid(xgbEstimator.nWorkers, Array(15))
//        .addGrid(xgbEstimator.subSample,Array(0.5))
        .build()
        val tv = new TrainValidationSplit()
//                .setEstimator(xgbEstimator)
//            .setEvaluator(evaluator)
                .setEvaluator(new RegressionEvaluator())
                .setEstimatorParamMaps(paramGrid)
                .setTrainRatio(0.8)

        val tvModel = tv.fit(train_df
                .withColumnRenamed(labelCol, "label")
            //使用对数进行转换
//                .withColumn("label",log(labelCol)+1).drop(labelCol)
        )
        tvModel
    }


}

object ModelUtils {

}