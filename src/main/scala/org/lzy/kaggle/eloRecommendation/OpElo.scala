package org.lzy.kaggle.eloRecommendation

import com.salesforce.op.evaluators.Evaluators
import com.salesforce.op.features.FeatureLike
import com.salesforce.op.features.types.Prediction
import com.salesforce.op.stages.impl.regression.{OpXGBoostRegressor, RegressionModelSelector}
import com.salesforce.op.stages.impl.regression.RegressionModelsToTry.{OpRandomForestRegressor, OpXGBoostRegressor}
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
    val models = Seq(
        xg -> new ParamGridBuilder()
                .addGrid(xg.eta, Array(0.3))
                .addGrid(xg.maxDepth, Array(3))
                .addGrid(xg.minChildWeight, Array(10.0))
                .addGrid(xg.numRound, Array(100))
                .build())
    xg.setSilent()
    val prediction: FeatureLike[Prediction] = RegressionModelSelector
            .withCrossValidation(
                dataSplitter = Some(DataSplitter(seed = randomSeed))
                //      ,modelTypesToUse = Seq( OpXGBoostRegressor)
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
                .setInputDataset[Record](test_ds, key = _.card_id)

        val model = workflow.loadModel(modelPath)

        val score_ds = model.score()
        score_ds
    }
}
