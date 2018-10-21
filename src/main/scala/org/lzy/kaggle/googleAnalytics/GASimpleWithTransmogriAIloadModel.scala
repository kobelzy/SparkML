package org.lzy.kaggle.googleAnalytics

import com.salesforce.op.OpWorkflow
import com.salesforce.op.features.FeatureLike
import com.salesforce.op.features.types.Prediction
import com.salesforce.op.readers.{CSVProductReader, DataReaders}
import com.salesforce.op.stages.impl.regression.RegressionModelSelector
import common.{SparkUtil, Utils}
import org.apache.spark.sql.Row

/**
  * Auther: lzy
  * Description:
  * Date Created by： 9:23 on 2018/10/9
  * Modified By：
  */
/*
spark-submit --master yarn-client --queue all --num-executors 16 --executor-memory 10g --driver-memory 3g --executor-cores 4 --packages com.salesforce.transmogrifai:transmogrifai-core_2.11:0.4.0 --class org.lzy.kaggle.googleAnalytics.GASimpleWithTransmogriAIloadModel SparkML.jar
 */
object GASimpleWithTransmogriAIloadModel extends CustomerFeatures {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkUtil.getSpark()
    spark.sparkContext.setLogLevel("warn")
    import spark.implicits._
    val prediction: FeatureLike[Prediction] = RegressionModelSelector
      .withCrossValidation()
      .setInput(totals_transactionRevenue, finalFeatures)
      .getOutput()
    val testDataReader: CSVProductReader[Customer] = DataReaders.Simple.csvCase[Customer](path = Option(Constants.testPath), key = v => v.fullVisitorId)
    val modelPath = Constants.modelPath
    val test_ds = testDataReader.readDataset()
    test_ds.show(false)
    val workflow = new OpWorkflow()
      .setResultFeatures(prediction, customerFeatures)
      //      .setReader(testDataReader)
      .setInputDataset(test_ds)

    val model = workflow.loadModel(modelPath)
      //      .setInputDataset(test_ds)
      .setReader(testDataReader)
    //    model.score(path=Option(Constants.resultPath)).show(false)
    val util = new Utils(spark)


    val prediction_df = model.score()
      .map(raw => {
        val fullVisitorId = raw.getString(0)
        val transactionRevenue = math.expm1(raw.getMap[String, Double](1).getOrElse("prediction", 0d))
        (fullVisitorId, transactionRevenue)
      }).toDF("fullVisitorId", "PredictedLogRevenue")
      .groupBy("fullVisitorId").sum("PredictedLogRevenue")
      .map{case Row(fullVisitorId:String,transactionRevenue:Double)=>
        (fullVisitorId,math.log1p(transactionRevenue))
      }
      .toDF("fullVisitorId", "PredictedLogRevenue")
    util.writeToCSV(prediction_df, Constants.basePath + "result/result.csv")
    //    println(model.summary())
    //    println("")
    //    //    println(model.summaryJson())
    //    println("-------------")
    //    println(model.summaryPretty())
    //    val indexedLabel: Feature[RealNN] = new OpStringIndexerNoFilter().setInput(label).getOutput
    //    val labelIndexer = fittedLeadWorkflow
    //      .getOriginStageOf(indexedLabel).asInstanceOf[OpStringIndexerNoFilter] //
    //    val lr:FeatureLike[Prediction]=new OpRandomForestRegressor().setInput(totals_transactionRevenue, customerFeatures) .getOutput()
    //  .setInput(totals_transactionRevenue, customerFeatures) .getOutput()
    //    lrModel.transform()
    //    model.computeDataUpTo(totals_transactionRevenue).show(false)
    //    val selectModel: SelectedModel = model.getOriginStageOf(prediction).asInstanceOf[SelectedModel]
    //    println(selectModel.extractParamMap())
    //    val pre_ds = selectModel.transform(test_ds)

    //    val op=toOP(selectModel,selectModel.uid).transform(pre_ds).show(false)
    //    SparkModelConverter.toOPUnchecked(selectModel).transform(pre_ds).show(false)
    //    SparkModelConverter.toOP(selectModel,selectModel.uid)
  }
}
