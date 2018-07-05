package org.lzy.kaggle.kaggleSantander

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.DoubleType

/**
  * Created by Administrator on 2018/6/18.
  */
class ScoreEvaluator(override val uid: String)
  extends Evaluator with DefaultParamsWritable {
  var labelCol = "label"
  var predictionCol = "prediction"

  def this() = this(Identifiable.randomUID("regEval"))

  override def evaluate(dataset: Dataset[_]): Double = {

    //   val schema = dataset.schema
    //   SchemaUtils.checkColumnTypes(schema, col(predictionCol), Seq(DoubleType, FloatType))
    //   SchemaUtils.checkNumericType(schema, col(labelCol))
    //
    //   val predictionAndLabels = dataset
    //     .select(col(predictionCol).cast(DoubleType), col(labelCol).cast(DoubleType))
    //     .rdd
    //     .map { case Row(prediction: Double, label: Double) => (prediction, label) }
    //   val metrics = new RegressionMetrics(predictionAndLabels)
    //   val metric = $(metricName) match {
    //    case "rmse" => metrics.rootMeanSquaredError
    //    case "mse" => metrics.meanSquaredError
    //    case "r2" => metrics.r2
    //    case "mae" => metrics.meanAbsoluteError
    //   }
    //   metric
    /**
      * s1分数
      */
    score(dataset, labelCol, predictionCol)

  }

  // def setPredictionCol(value: String): this.type = set(predictionCol, value)
  // def setLabelCol(value: String): this.type = set(labelCol, value)
  def setPredictionCol(value: String) = this.predictionCol = value

  def setLabelCol(value: String) = this.labelCol = value


  override def copy(extra: ParamMap): Evaluator = defaultCopy(extra)


  def score(result_df: Dataset[_], labelCol: String, predictionCol: String) = {
    val n = result_df.count()
    val udf_getWeight = udf { (label: Double, prediction: Double) => math.pow(math.log1p(label) - math.log1p(prediction), 2) }
    result_df.withColumn("score", udf_getWeight(col(labelCol), col(predictionCol)))
    val score=math.sqrt(result_df.select(udf_getWeight(col(labelCol), col(predictionCol))).collect().map(_.getDouble(0)).sum)
    score
    //  0.4 * s1 + 0.6 * s2
  }
}




