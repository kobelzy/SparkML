package org.lzy.kaggle.eloRecommendation

import common.SparkUtil
import org.lzy.kaggle.eloRecommendation.DataCollect.{collectTransaction, extractTranAndTest}

object Run {
  val spark=SparkUtil.getSpark()
  spark.sparkContext.setLogLevel("WARN")
import spark.implicits._
  def main(args: Array[String]): Unit = {
run()
  }

  def run()={

    val (new_feature_df,authorized_feature_df,history_df)= collectTransaction
    val   (train_df,test_df) =extractTranAndTest

    val train=train_df.join(new_feature_df,Seq("card_id"),"left")
      .join(authorized_feature_df,Seq("card_id"),"left")
      .join(history_df,Seq("card_id"),"left")
        .na.drop()
    train.show(false)
    val train_ds=train.as[Record]
    OpElo.trainModel(train_ds)
  }
}
