package org.lzy.JData.JDataByLeaner

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{Dataset, Row}

/**
  * Created by Administrator on 2018/6/18.
  */
 class UDLogisticEvaluator(override val uid: String)
  extends Evaluator with DefaultParamsWritable{
 var labelCol="label"
 var predictionCol="prediction"
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
   score(dataset,labelCol,predictionCol)

  }
// def setPredictionCol(value: String): this.type = set(predictionCol, value)
// def setLabelCol(value: String): this.type = set(labelCol, value)
def setPredictionCol(value: String) = this.predictionCol=value
 def setLabelCol(value: String) = this.labelCol=value


  override def copy(extra: ParamMap): Evaluator = defaultCopy(extra)


 def score(result_df: Dataset[_],labelCol:String,predictionCol:String) = {

  val udf_getWeight = udf { index: Int => 1.0 / (1 + math.log(index)) }
  //


  //按照label2预测的结果进行排序。
  val weight_df = result_df.coalesce(1).sort(col(predictionCol).desc)
    .limit(50000)
    .withColumn("label_binary", when(col(labelCol) > 0, 1.0).otherwise(0.0))
    //      .withColumn("label_binary", udf_binary($"label_1"))
    .withColumn("index", monotonically_increasing_id + 1)
    .withColumn("weight", udf_getWeight(col("index")))

  val s1 = weight_df.select("label_binary", "weight")
      .rdd
    .map{case Row(label_binary: Double, weight: Double) => label_binary * weight}.collect().sum / 4674.32357
  //1 to 50000 map(i=>1.0/(1+math.log(i)))
  //计算s2
//  val weightEqual1_df = result_df.filter(col("label_1") > 0)
//  val s2 = weight_df.filter(col("label_1") > 0).select(col("label_2").as[Double], col("pred_date").as[Double]).collect().map { case (label_2, pred_date) =>
//   10.0 / (math.pow(label_2 - math.round(pred_date), 2) + 10)
//  }.sum / weightEqual1_df.count().toDouble
  1.0/s1

//  0.4 * s1 + 0.6 * s2
 }
}


object UDLogisticEvaluator extends DefaultParamsReadable[UDLogisticEvaluator] {
 override def load(path: String): UDLogisticEvaluator = super.load(path)
}

