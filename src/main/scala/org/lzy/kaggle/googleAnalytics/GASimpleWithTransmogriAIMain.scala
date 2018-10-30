package org.lzy.kaggle.googleAnalytics

import com.salesforce.op.aggregators.CutOffTime
import com.salesforce.op.{ModelInsights, OpWorkflow, OpWorkflowModel}
import com.salesforce.op.evaluators.{Evaluators, OpRegressionEvaluator}
import com.salesforce.op.features.FeatureLike
import com.salesforce.op.features.types._
import com.salesforce.op.readers.{AggregateParams, CSVProductReader, DataReaders}
import com.salesforce.op.stages.impl.regression.RegressionModelSelector
import com.salesforce.op.stages.impl.regression.RegressionModelsToTry.{OpGBTRegressor, OpRandomForestRegressor}
import com.salesforce.op.stages.impl.selector.SelectedModel
import com.salesforce.op.stages.impl.tuning.DataSplitter
import common.{SparkUtil, Utils}
import org.apache.spark.sql.SparkSession
import spire.std.option

/*

spark-submit --master yarn-cluster --queue all \
--num-executors 16 \
--executor-memory 10g \
--driver-memory 3g \
--executor-cores 4 \
--packages com.salesforce.transmogrifai:transmogrifai-core_2.11:0.4.0,joda-time:joda-time:2.10.1 \
--class org.lzy.kaggle.googleAnalytics.GASimpleWithTransmogriAIMain SparkML.jar */

object GASimpleWithTransmogriAIMain extends CustomerFeatures {


  def main(args: Array[String]): Unit = {
    run()
  }

  def run() = {
    implicit val spark: SparkSession = SparkUtil.getSpark()
    spark.sparkContext.setLogLevel("warn")
    import spark.implicits._


    ////////////////////////////////////////////////////////////////////////////////
    //定义测试模型集
    /////////////////////////////////////////////////////////////////////////////////
    val randomSeed = 112233L

    val prediction: FeatureLike[Prediction] =
      RegressionModelSelector
        .withCrossValidation(
          dataSplitter = Some(DataSplitter(seed = randomSeed)), seed = randomSeed
//                ,modelTypesToUse = Seq( OpRandomForestRegressor)
        )
        //RandomForestRegression, LinearRegression, GBTRegression
        .setInput(totals_transactionRevenue, finalFeatures).getOutput()

    val trainDataReader: CSVProductReader[Customer] =
      DataReaders.Simple.csvCase[Customer](path = Option(Constants.trainPath), key = v => v.fullVisitorId)
//    DataReaders.Aggregate.csvCase[Customer](path = Option(Constants.trainPath),
//      key = v => v.fullVisitorId,
//      aggregateParams = AggregateParams(
//        timeStampFn = Some[Customer => Long](s => s.date),
//        cutOffTime = CutOffTime.NoCutoff()
//      )
//    )

    //    val testDataReader: CSVProductReader[Customer] = DataReaders.Simple.csvCase[Customer](path = Option(Constants.testPath), key = v => v.fullVisitorId)
    //    val util=new Utils(spark)
    //    val train_DS=util.readToCSV(Constants.trainPath).as[Customer]

    trainDataReader.readDataset().show(false)
    ////////////////////////////////////////////////////////////////////////////////
    // WORKFLOW训练及保存
    /////////////////////////////////////////////////////////////////////////////////

    val workflow = new OpWorkflow()
      .setResultFeatures(prediction)
      .setReader(trainDataReader)
    //      .setInputDataset(train_DS)
    //            .withRawFeatureFilter(Some(trainDataReader),Some(testDataReader))
    val fittedWorkflow: OpWorkflowModel = workflow.train()
    fittedWorkflow.save(Constants.modelPath, true)
    ////////////////////////////////////////////////////////////////////////////////
    //模型评估
    /////////////////////////////////////////////////////////////////////////////////
    //    println(s"Summary: ${fittedWorkflow.summary()}")
    println("Model summary:\n" + fittedWorkflow.summaryPretty())
    ////    // Manifest the result features of the workflow
    ////    println("Scoring the model")
    //    val (dataframe, metrics) = fittedWorkflow.scoreAndEvaluate(evaluator = evaluator)
    //
    //    println("Transformed dataframe columns:")
    //    dataframe.columns.foreach(println)
    //    println("Metrics:")
    //    println(metrics)
    val evaluator = Evaluators.Regression()
      .setLabelCol(totals_transactionRevenue)
      .setPredictionCol(prediction)

    println("均方根误差:" + fittedWorkflow.evaluate(evaluator = evaluator).RootMeanSquaredError)
  }
}
