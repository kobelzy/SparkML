package org.lzy.kaggle.eloRecommendation

object EloConstants {
  var basePath = "hdfs://10.95.3.172:9000/user/lzy/EloRecommendation/"
  if (System.getProperty("os.name").toLowerCase().indexOf("windows") != -1) {
    basePath = "D:/Dataset/EloRecommendation/"
    //         basePath = "hdfs://10.95.3.172:9000/user/lzy/GoogleAnalyse/"
  }

  val trainPath = basePath + "source/train.csv"
  val testPath = basePath + "source/test.csv"
  val trainPath_mini = basePath + "source/train_mini.csv"
  val testPath_mini = basePath + "source/test_mini.csv"

  val historical=basePath+"source/historical_transactions.csv"
  val newMerChantTransactions=basePath+"source/new_merchant_transactions.csv"
  val historical_mini=basePath+"source/historical_transactions_mini.csv"
  val newMerChantTransactions_mini=basePath+"source/new_merchant_transactions_mini.csv"

  val merchants=basePath+"source/merchants.csv"

  val modelPath=basePath + "model/bestMode"


  val resultPath=basePath+"result/submission"
}
