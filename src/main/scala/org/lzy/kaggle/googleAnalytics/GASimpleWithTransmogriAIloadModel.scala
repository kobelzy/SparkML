package org.lzy.kaggle.googleAnalytics

import com.salesforce.op._
import com.salesforce.op.features.types._
import com.salesforce.op.features.{FeatureBuilder, FeatureLike}
import com.salesforce.op.features.types.Prediction
import com.salesforce.op.stages.impl.regression.{OpRandomForestRegressionModel, OpRandomForestRegressor, RegressionModelSelector}
import com.salesforce.op.stages.impl.tuning.DataSplitter
import com.salesforce.op.{OpWorkflow, OpWorkflowModel, OpWorkflowModelReadWriteShared}
import common.SparkUtil
import java.text.SimpleDateFormat

import com.salesforce.op.readers.{CSVProductReader, DataReaders}
import com.salesforce.op.stages.impl.feature.OpStringIndexerNoFilter
import com.salesforce.op.stages.impl.selector.SelectedModel
import com.salesforce.op.stages.sparkwrappers.specific.SparkModelConverter
import com.salesforce.op.utils.stages.FitStagesUtil
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.regression.RegressionModel
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}
/**
  * Auther: lzy
  * Description:
  * Date Created by： 9:23 on 2018/10/9
  * Modified By：
  */

object GASimpleWithTransmogriAIloadModel extends CustomerFeatures {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkUtil.getSpark()
    import spark.implicits._
    val prediction: FeatureLike[Prediction] = RegressionModelSelector
      .withCrossValidation()
      .setInput(totals_transactionRevenue, customerFeatures)
      .getOutput()
    val testDataReader: CSVProductReader[Customer] = DataReaders.Simple.csvCase[Customer](path = Option(Constants.testPath), key = v => v.fullVisitorId + "")
    testDataReader.readDataset().show(false)
    val modelPath = Constants.basePath + "model/bestModel"
    val test_ds = testDataReader.readDataset()
      test_ds.show(false)
    val workflow = new OpWorkflow()
      .setResultFeatures(prediction,customerFeatures)
//      .setReader(testDataReader)
          .setInputDataset(test_ds)
    val model = workflow.loadModel(modelPath)
//      .setInputDataset(test_ds)
        .setReader(testDataReader)
    //        model.score()
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
    model.computeDataUpTo(totals_transactionRevenue).show(false)
    val selectModel: SelectedModel = model.getOriginStageOf(prediction).asInstanceOf[SelectedModel]
    println(selectModel.extractParamMap())
    val pre_ds = selectModel.transform(test_ds)
    pre_ds.show(false)


//    val op=toOP(selectModel,selectModel.uid).transform(pre_ds).show(false)
    SparkModelConverter.toOPUnchecked(selectModel).transform(pre_ds).show(false)
//    SparkModelConverter.toOP(selectModel,selectModel.uid)
  }
}
