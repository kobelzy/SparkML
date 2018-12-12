package org.lzy.kaggle.eloRecommendation

import com.salesforce.op.features.FeatureLike
import com.salesforce.op.features.types.Prediction
import com.salesforce.op.stages.impl.regression.RegressionModelSelector
import com.salesforce.op.stages.impl.regression.RegressionModelsToTry.OpRandomForestRegressor
import com.salesforce.op.stages.impl.tuning.DataSplitter
import com.salesforce.op.{OpWorkflow, OpWorkflowModel}
import common.SparkUtil
import org.apache.spark.sql.{DataFrame, Dataset}

object OpElo extends RecordFeatures {
 implicit val spark=Run.spark
  import spark.implicits._
  def trainModel(train_df:Dataset[Record])={
    val randomSeed = 112233L

    ////////////////////////////////////////////////////////////////////////////////
    //定义测试模型集
    /////////////////////////////////////////////////////////////////////////////////

    val prediction:FeatureLike[Prediction]=RegressionModelSelector
      .withCrossValidation(
        dataSplitter = Some(DataSplitter(seed=randomSeed))
        ,modelTypesToUse = Seq( OpRandomForestRegressor)
      )
      .setInput(target,features)
      .getOutput()

    ////////////////////////////////////////////////////////////////////////////////
    // WORKFLOW DEFINITION
    /////////////////////////////////////////////////////////////////////////////////
    val workflow=new OpWorkflow()
      .setInputDataset(train_df)
      .setResultFeatures(prediction)

    val model:OpWorkflowModel=workflow.train()
    println(model.summary())
  }
}
