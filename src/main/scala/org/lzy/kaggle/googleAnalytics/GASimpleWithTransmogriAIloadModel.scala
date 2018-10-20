package org.lzy.kaggle.googleAnalytics

import com.salesforce.op.OpWorkflow
import com.salesforce.op.features.FeatureLike
import com.salesforce.op.features.types.Prediction
import com.salesforce.op.readers.{CSVProductReader, DataReaders}
import com.salesforce.op.stages.impl.regression.RegressionModelSelector
import com.salesforce.op.stages.impl.selector.SelectedModel
import com.salesforce.op.stages.sparkwrappers.specific.SparkModelConverter
import common.{SparkUtil, Utils}
/**
  * Auther: lzy
  * Description:
  * Date Created by： 9:23 on 2018/10/9
  * Modified By：
  */

object GASimpleWithTransmogriAIloadModel extends CustomerFeatures {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkUtil.getSpark()
    spark.sparkContext.setLogLevel("warn")
    import spark.implicits._
    val prediction: FeatureLike[Prediction] = RegressionModelSelector
      .withCrossValidation()
      .setInput(totals_transactionRevenue, customerFeatures)
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
    val prediction_df = model.score()
      .map(raw => {
        val fullVisitorId = raw.getString(0)
        val transactionRevenue = raw.getMap[String, Double](1).getOrElse("prediction", 0d).formatted("%.4f")
        (fullVisitorId, transactionRevenue)
      }).toDF("fullVisitorId", "transactionRevenue")
    val util = new Utils(spark)
    prediction_df.show(false)
    util.writeToCSV(prediction_df, Constants.resultPath)



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
