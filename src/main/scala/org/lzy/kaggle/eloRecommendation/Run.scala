package org.lzy.kaggle.eloRecommendation

import common.{DataUtils, SparkUtil}
import ml.dmlc.xgboost4j.scala.spark.TrackerConf
import org.apache.spark.sql.SaveMode
import org.lzy.kaggle.eloRecommendation.DataCollect.{collectTransaction, extractTranAndTest}

/*


spark-submit --master yarn-cluster --queue all \
--num-executors 10 \
--executor-memory 18g \
--driver-memory 10g \
--executor-cores 4 \
--packages com.salesforce.transmogrifai:transmogrifai-core_2.11:0.5.0,joda-time:joda-time:2.10.1 \
--class org.lzy.kaggle.eloRecommendation.Run SparkML.jar


 */
object Run {
  val spark = SparkUtil.getSpark()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val dataUtils = new DataUtils(spark)

  def main(args: Array[String]): Unit = {

        aggreDate
    trainModel
    predict
    //    OpElo.showSummary(EloConstants.modelPath)
  }

  def aggreDate() = {
    val (trainPath, testPath, historicalPath, newTransactionPath) = if (System.getProperty("os.name").toLowerCase().indexOf("windows") != -1)
      (EloConstants.trainPath_mini, EloConstants.testPath_mini, EloConstants.historical_mini, EloConstants.newMerChantTransactions_mini)
    else (EloConstants.trainPath, EloConstants.testPath, EloConstants.historical, EloConstants.newMerChantTransactions)

    val (new_feature_df, authorized_feature_df, history_df) = collectTransaction(historicalPath, newTransactionPath)


    val (train_df, test_df) = extractTranAndTest(trainPath, testPath)
    //    new_feature_df.show(false)
    //    authorized_feature_df.show(false)
    //    history_df.show(false)
    val train = train_df.join(new_feature_df, Seq("card_id"), "left")
      .join(authorized_feature_df, Seq("card_id"), "left")
      .join(history_df, Seq("card_id"), "left")

    val train_ds = train.as[Record]
    train_ds.show(false)
    train_ds.write.mode(SaveMode.Overwrite).parquet(EloConstants.basePath + "cache/train_ds")

    val test = test_df.join(new_feature_df, Seq("card_id"), "left")
      .join(authorized_feature_df, Seq("card_id"), "left")
      .join(history_df, Seq("card_id"), "left")

    val test_ds = test.as[Record]

    test_ds.write.mode(SaveMode.Overwrite).parquet(EloConstants.basePath + "cache/test_ds")
  }

  def trainModel() = {

    val train_ds = spark.read.parquet(EloConstants.basePath + "cache/train_ds").as[Record]
    //        .filter(_.target > -30)
    val model = OpElo.trainModel(train_ds)
    model.save(EloConstants.modelPath, true)
    println("Model summary:\n" + model.summaryPretty())
    OpElo.evaluateModel(model)
  }

  def predict() = {

    val test_ds = spark.read.parquet(EloConstants.basePath + "cache/test_ds").as[Record]

    case class Submission(card_id: String, target: Double)
    val submission_ds = OpElo.predict(test_ds, EloConstants.modelPath)
      .map(raw => {
        val cardId = raw.getString(0)
        val target = raw.getMap[String, Double](1).getOrElse("prediction", 0d)
        (cardId, target)
      })
      .toDF("card_id", "target")
    dataUtils.to_csv(submission_ds, EloConstants.resultPath)

  }
}
