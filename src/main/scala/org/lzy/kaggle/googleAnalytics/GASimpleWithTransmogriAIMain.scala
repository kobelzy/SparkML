package org.lzy.kaggle.googleAnalytics

import java.text.SimpleDateFormat

import com.salesforce.op.evaluators.Evaluators
import com.salesforce.op.features.types._
import com.salesforce.op.features.{FeatureBuilder, FeatureLike}
import com.salesforce.op.readers.DataReaders
import com.salesforce.op.stages.impl.regression.RegressionModelSelector
import com.salesforce.op.stages.impl.regression.RegressionModelsToTry._
import com.salesforce.op.stages.impl.tuning.DataSplitter
import com.salesforce.op.{OpWorkflow, _}
import common.SparkUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/*

spark-submit --master yarn-client --queue all \
--num-executors 15 \
--executor-memory 7g \
--driver-memory 3g \
--executor-cores 3 \
--packages com.salesforce.transmogrifai:transmogrifai-core_2.11:0.4.0 \
--class org.lzy.kaggle.googleAnalytics.GASimpleWithTransmogriAI SparkML.jar */

object GASimpleWithTransmogriAIMain extends CustomerFeatures {


  def main(args: Array[String]): Unit = {
    run()
  }

  def run() = {
    implicit val spark: SparkSession = SparkUtil.getSpark()
//    spark.sparkContext.setLogLevel("warn")
    import spark.implicits._



    ////////////////////////////////////////////////////////////////////////////////
    //定义测试模型集
    /////////////////////////////////////////////////////////////////////////////////
    val randomSeed = 112233L

    val prediction: FeatureLike[Prediction] =
    RegressionModelSelector
      .withCrossValidation(
      dataSplitter = Some(DataSplitter(seed = randomSeed)), seed = randomSeed
//      ,modelTypesToUse = Seq(OpGBTRegressor, OpRandomForestRegressor)
 )
      //RandomForestRegression, LinearRegression, GBTRegression
      .setInput(totals_transactionRevenue, customerFeatures).getOutput()
    val evaluator = Evaluators.Regression()
      .setLabelCol(totals_transactionRevenue)
      .setPredictionCol(prediction)
    val trainDataReader = DataReaders.Simple.csvCase[Customer](path = Option(Constants.trainPath), key = v => v.fullVisitorId + "_" + v.sessionId)
    ////////////////////////////////////////////////////////////////////////////////
    // WORKFLOW
    /////////////////////////////////////////////////////////////////////////////////

    val workflow = new OpWorkflow()
            .setResultFeatures(prediction)
//      .setReader(trainDataReader)

    val model=workflow.loadModel(Constants.basePath+"model/bestModel")
    println(model.summary())
//    val fittedWorkflow:OpWorkflowModel = workflow.train()
//    fittedWorkflow.save(basePath+"model/bestModel",true)
//    println("Model summary:\n" + fittedWorkflow.summaryPretty())
////    println(s"Summary: ${fittedWorkflow.summary()}")
////    // Manifest the result features of the workflow
////    println("Scoring the model")
//    val (dataframe, metrics) = fittedWorkflow.scoreAndEvaluate(evaluator = evaluator)
//
//    println("Transformed dataframe columns:")
//    dataframe.columns.foreach(println)
//    println("Metrics:")
//    println(metrics)
  }
}
