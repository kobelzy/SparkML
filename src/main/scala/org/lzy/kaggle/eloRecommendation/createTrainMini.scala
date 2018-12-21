package org.lzy.kaggle.eloRecommendation

import common.{DataUtils, SparkUtil}
import org.apache.spark.util.SparkUtils

object createTrainMini {
  def main(args: Array[String]): Unit = {
    make()
  }
def make()={
  val spark=SparkUtil.getSpark()
val utils=new DataUtils(spark)
  spark.sparkContext.setLogLevel("WARN")
  val hist=utils.read_csv(EloConstants.historical_mini).select("card_id")
  val new_df=utils.read_csv(EloConstants.newMerChantTransactions_mini).select("card_id")
  val cards=hist.union(new_df).distinct()
  val train=utils.read_csv(EloConstants.trainPath)
  val result=train.join(cards,"card_id").select("first_active_month","card_id",        "feature_1","feature_2","feature_3","target" )
  train.show(false)
  result.show(false)
  utils.to_csv(result,EloConstants.trainPath+"_mini")
}
}
