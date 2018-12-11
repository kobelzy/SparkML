package org.lzy.kaggle.eloRecommendation

import com.salesforce.op.stages.impl.regression.RegressionModelSelector
import com.salesforce.op.stages.impl.tuning.DataSplitter
import com.salesforce.op.{OpWorkflow, OpWorkflowModel}
import common.SparkUtil
import org.apache.spark.sql.DataFrame

object OpElo {
 implicit val spark=SparkUtil.getSpark()
  import spark.implicits._
  def trainModel(train_df:DataFrame)={
    val randomSeed = 112233L

    ////////////////////////////////////////////////////////////////////////////////
    //定义测试模型集
    /////////////////////////////////////////////////////////////////////////////////

    val prediction=RegressionModelSelector
      .withCrossValidation(
        dataSplitter = Some(DataSplitter(seed=randomSeed)),seed=randomSeed
      )

    ////////////////////////////////////////////////////////////////////////////////
    // WORKFLOW DEFINITION
    /////////////////////////////////////////////////////////////////////////////////
    val workflow=new OpWorkflow()
      .setInputDataset(train_df)


    val model:OpWorkflowModel=workflow.train()
  }
}
