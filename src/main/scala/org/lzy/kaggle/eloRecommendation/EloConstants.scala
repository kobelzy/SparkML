package org.lzy.kaggle.eloRecommendation

object EloConstants {
  var basePath = "hdfs://10.95.3.172:9000/user/lzy/EloRecommendation/"
  if (System.getProperty("os.name").toLowerCase().indexOf("windows") != -1) {
    basePath = "D:/Dataset/EloRecommendation/"
    //         basePath = "hdfs://10.95.3.172:9000/user/lzy/GoogleAnalyse/"
  }

  val trainPath = basePath + "source/train.csv"
  val testPath = basePath + "source/test.csv"
  val historical=basePath+"source/historical_transactions.csv"
  val merchants=basePath+"source/merchants.csv"
  val newMerChantTransactions=basePath+"source/new_merchant_transactions.csv"



  val modelPath=basePath + "model/bestModel"
  val modelPath2=basePath+"model/bestModel2"

  val resultPath=basePath+"result/result.csv"
}
