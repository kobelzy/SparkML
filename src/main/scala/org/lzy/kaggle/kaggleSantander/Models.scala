package org.lzy.kaggle.kaggleSantander

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Administrator on 2018/7/3.
  */
class Models(spark:SparkSession) {
import spark.implicits._
  def LR_TranAndSave(data:DataFrame,label:String="label",features:String="features")={
    val lr=new LinearRegression()
      .setMaxIter(100)
      .setLabelCol(label)
      .setFeaturesCol(features)

    val lr_model=lr.fit(data)
    lr_model.write.overwrite().save(Constant.basePath+"model/lr_model")
    lr_model
  }


}
