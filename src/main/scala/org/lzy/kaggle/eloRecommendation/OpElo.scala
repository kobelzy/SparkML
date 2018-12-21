package org.lzy.kaggle.eloRecommendation

import com.salesforce.op.evaluators.Evaluators
import com.salesforce.op.features.FeatureLike
import com.salesforce.op.features.types.Prediction
import com.salesforce.op.stages.impl.regression.{OpDecisionTreeRegressor, OpRandomForestRegressor, OpXGBoostRegressor, RegressionModelSelector}
import com.salesforce.op.stages.impl.regression.RegressionModelsToTry._
import com.salesforce.op.stages.impl.selector.DefaultSelectorParams
import com.salesforce.op.stages.impl.tuning.DataSplitter
import com.salesforce.op.{OpWorkflow, OpWorkflowModel}
import common.SparkUtil
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.{DataFrame, Dataset}
import org.lzy.transmogriAI.boston.OpBoston.lr

object OpElo extends RecordFeatures {
    implicit val spark = Run.spark

    import spark.implicits._

    val randomSeed = 112233L
    ////////////////////////////////////////////////////////////////////////////////
    //定义测试模型集
    /////////////////////////////////////////////////////////////////////////////////
    val xg = new OpXGBoostRegressor()
    val rf=new OpRandomForestRegressor()
    val models = Seq(
//        xg -> new ParamGridBuilder()
//                .addGrid(xg.eta, Array(0.3))
//                .addGrid(xg.maxDepth, Array(3))
//                .addGrid(xg.minChildWeight, Array(10.0))
//                .addGrid(xg.numRound, Array(100))
//                .build(),
    rf->new ParamGridBuilder()
      .addGrid(rf.maxDepth,  Array(3, 6, 12) )// for trees spark default 5
      .addGrid(rf.maxBins, Array(32) )// bins for cont variables in trees - 32 is spark default
      .addGrid(rf.minInfoGain, Array(0.001, 0.01, 0.1)) // spark default 0
      .addGrid(rf.minInstancesPerNode, Array(10, 100)) // spark default 1
      .addGrid(rf.numTrees,  Array(20,50)) // spark default 20
      .addGrid(rf.subsamplingRate, Array(1.0)) // sample of data used for tree fits spark default 1.0
      .build()

    )
    val prediction: FeatureLike[Prediction] = RegressionModelSelector
            .withCrossValidation(
                dataSplitter = Some(DataSplitter(seed = randomSeed))
//                      ,modelTypesToUse = Seq( OpRandomForestRegressor,OpLinearRegression,OpGBTRegressor)
                , modelsAndParameters = models
            )
            .setInput(target, features)
            .getOutput()

    def trainModel(train_ds: Dataset[Record]) = {


        ////////////////////////////////////////////////////////////////////////////////
        // WORKFLOW DEFINITION
        /////////////////////////////////////////////////////////////////////////////////
        val workflow = new OpWorkflow()
                .setResultFeatures(prediction)
                .setInputDataset[Record](train_ds, key = _.card_id)

        val model: OpWorkflowModel = workflow.train()
        model
    }

    def evaluateModel(model: OpWorkflowModel) = {
        val evaluator = Evaluators.Regression()
                .setLabelCol(target)
                .setPredictionCol(prediction)

        println("均方根误差:" + model.evaluate(evaluator = evaluator).RootMeanSquaredError)
    }


    def predict(test_ds: Dataset[Record], modelPath: String) = {
        val workflow = new OpWorkflow()
                .setResultFeatures(prediction, target)
//                .setInputDataset[Record](test_ds, key = _.card_id)

        val model = workflow.loadModel(modelPath)
                .setInputDataset[Record](test_ds, key = _.card_id)

        println("Model summary:\n" + model.summaryPretty())

        val score_ds = model.score()
        score_ds
    }

    def showSummary( modelPath: String) = {
        val workflow = new OpWorkflow()
                .setResultFeatures(prediction, target)

        val model = workflow.loadModel(modelPath)
        println("Model summary:\n" + model.summaryPretty())

    }
}
