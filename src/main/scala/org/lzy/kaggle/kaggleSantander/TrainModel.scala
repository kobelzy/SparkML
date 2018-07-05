package org.lzy.kaggle.kaggleSantander

import common.{FeatureUtils, Utils}
import org.apache.spark.ml.feature.ChiSqSelectorModel
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{abs, udf}
import org.apache.spark.ml.linalg
/**
  * Created by Administrator on 2018/7/3.
  */
object TrainModel{
  def main(args: Array[String]): Unit = {

  }


}
class TrainModel(spark:SparkSession) {
  import spark.implicits._
def testChiSq(train_df:DataFrame,fdr:Double)={
  val models=new Models(spark)

  val featureFilterColumns_arr=Array("id","target")
  val featureColumns_arr=train_df.columns.filterNot(column=>featureFilterColumns_arr.contains(column.toLowerCase))
  var stages:Array[PipelineStage]=FeatureUtils.vectorAssemble(featureColumns_arr,"assmbleFeatures")
  stages=stages:+  FeatureUtils.chiSqSelectorByfdr("target","assmbleFeatures","features",fdr)
  val pipeline=new Pipeline().setStages(stages)

  val pipelin_model=pipeline.fit(train_df)
  val train_willFit_df=pipelin_model.transform(train_df).select("ID","target","features").withColumn("target",$"target"/10000d)

  val ChiSqSelectNums=train_willFit_df.select("features").take(1).map(_.getAs[linalg.Vector](0).size)
  val score=models.evaluateGDBT(train_willFit_df,"target")
  print(s"当前错误率上限：${fdr},已选择特征数量：$ChiSqSelectNums,score：${score}")
}
}
