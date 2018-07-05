package org.lzy.kaggle.kaggleSantander

import common.{FeatureUtils, Utils}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{abs, udf}

/**
  * Created by Administrator on 2018/7/3.
  */
object TrainModel{
  def main(args: Array[String]): Unit = {

  }


}
class TrainModel(spark:SparkSession) {
  import spark.implicits._
def trainModel()={
  val utils=new Utils(spark)
  val models=new Models(spark)
  val train_df=utils.readToCSV(Constant.basePath+"AData/train.csv").repartition(100).cache()

  val featureFilterColumns_arr=Array("id","target")
  val featureColumns_arr=train_df.columns.filterNot(column=>featureFilterColumns_arr.contains(column.toLowerCase))
  var stages:Array[PipelineStage]=FeatureUtils.vectorAssemble(featureColumns_arr,"assmbleFeatures")
  stages=stages:+  FeatureUtils.chiSqSelector("target","assmbleFeatures","features",1000)
  val pipeline=new Pipeline().setStages(stages)

  val pipelin_model=pipeline.fit(train_df)
  val train_willFit_df=pipelin_model.transform(train_df).select("ID","target","features").withColumn("target",$"target"/10000d)

  val test_willFit_df=pipelin_model.transform(test_df).select("id","features")

  val lr_model=models.LR_TranAndSave(train_willFit_df,"target")
  val format_udf=udf{prediction:Double=>
    "%08.9f".format(prediction)
  }
  val result_df=lr_model.transform(test_willFit_df).withColumn("target",format_udf(abs($"prediction"*10000d)))
    .select("id","target")
  utils.writeToCSV(result_df,Constant.basePath+s"submission/lr_${System.currentTimeMillis()}")
}
}
