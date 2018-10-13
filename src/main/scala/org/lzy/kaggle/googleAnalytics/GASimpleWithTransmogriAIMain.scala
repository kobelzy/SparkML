package org.lzy.kaggle.googleAnalytics

import com.salesforce.op.{ModelInsights, OpWorkflow, OpWorkflowModel}
import com.salesforce.op.evaluators.Evaluators
import com.salesforce.op.features.FeatureLike
import com.salesforce.op.features.types._
import com.salesforce.op.readers.DataReaders
import com.salesforce.op.stages.impl.regression.RegressionModelSelector
import com.salesforce.op.stages.impl.selector.SelectedModel
import com.salesforce.op.stages.impl.tuning.DataSplitter
import common.SparkUtil
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
    val trainDataReader = DataReaders.Simple.csvCase[Customer](path = Option(Constants.trainPath), key = v => v.fullVisitorId+"")
    ////////////////////////////////////////////////////////////////////////////////
    // WORKFLOW训练及保存
    /////////////////////////////////////////////////////////////////////////////////

    val workflow = new OpWorkflow()
            .setResultFeatures(prediction)
      .setReader(trainDataReader)
    val fittedWorkflow:OpWorkflowModel = workflow.train()
    fittedWorkflow.save(Constants.basePath+"model/bestModel",true)
    ////////////////////////////////////////////////////////////////////////////////
    //模型评估
    /////////////////////////////////////////////////////////////////////////////////
    println(s"Summary: ${fittedWorkflow.summary()}")
    println("Model summary:\n" + fittedWorkflow.summaryPretty())
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
