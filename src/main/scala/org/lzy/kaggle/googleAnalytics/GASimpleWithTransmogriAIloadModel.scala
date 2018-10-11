package org.lzy.kaggle.googleAnalytics

import com.salesforce.op._
import com.salesforce.op.features.types._
import com.salesforce.op.features.{FeatureBuilder, FeatureLike}
import com.salesforce.op.features.types.Prediction
import com.salesforce.op.stages.impl.regression.RegressionModelSelector
import com.salesforce.op.stages.impl.tuning.DataSplitter
import com.salesforce.op.{OpWorkflow, OpWorkflowModel, OpWorkflowModelReadWriteShared}
import common.SparkUtil
import java.text.SimpleDateFormat
import scala.util.{Failure, Success, Try}

/**
  * Auther: lzy
  * Description:
  * Date Created by： 9:23 on 2018/10/9
  * Modified By：
  */

object GASimpleWithTransmogriAIloadModel extends CustomerFeatures {
    def main(args: Array[String]): Unit = {
        val spark = SparkUtil.getSpark()

        val prediction: FeatureLike[Prediction] = RegressionModelSelector
                .withCrossValidation()
                .setInput(totals_transactionRevenue, customerFeatures)
                .getOutput()

        val modelPath = Constants.basePath + "model/bestModel"
        val workflow = new OpWorkflow()
                .setResultFeatures(prediction)
        val model = workflow.loadModel(modelPath)
//        model.score()
        println(model.summary())


    }
}
