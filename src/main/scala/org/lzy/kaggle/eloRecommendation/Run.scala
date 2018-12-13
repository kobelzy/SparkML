package org.lzy.kaggle.eloRecommendation

import common.SparkUtil
import org.lzy.kaggle.eloRecommendation.DataCollect.{collectTransaction, extractTranAndTest}

/*


spark-submit --master yarn-cluster --queue all \
--num-executors 15 \
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

  def main(args: Array[String]): Unit = {
    trainModel()
  }

  def trainModel() = {

    val (new_feature_df, authorized_feature_df, history_df) = collectTransaction(EloConstants.historical,EloConstants.newMerChantTransactions)
    val (train_df, test_df) = extractTranAndTest
    //    new_feature_df.show(false)
    //    authorized_feature_df.show(false)
    //    history_df.show(false)
    val train = train_df.join(new_feature_df, Seq("card_id"), "left")
      .join(authorized_feature_df, Seq("card_id"), "left")
      .join(history_df, Seq("card_id"), "left")
      .na.drop()

    train.show(false)

    val train_ds = train.as[Record]
    train_ds.write.parquet(EloConstants.basePath + "source/train_ds")
    val model = OpElo.trainModel(train_ds)
    model.save(EloConstants.basePath + "model/model1")
    println("Model summary:\n" + model.summaryPretty())
    OpElo.evaluateModel(model)

  }
}
