package org.lzy.kaggle.kaggleSantander

import common.{FeatureUtils, Utils}
import org.apache.spark.ml.feature.ChiSqSelectorModel
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
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
  val utils=new Utils(spark)
def testChiSqByFdr(train_df:DataFrame,fdr:Double)={
  val models=new Models(spark)
  val featureColumns_arr=train_df.columns.filterNot(column=>Constant.featureFilterColumns_arr.contains(column.toLowerCase))
  var stages:Array[PipelineStage]=FeatureUtils.vectorAssemble(featureColumns_arr,"assmbleFeatures")
  stages=stages:+  FeatureUtils.chiSqSelectorByfdr("target","assmbleFeatures","features",fdr)
  val pipeline=new Pipeline().setStages(stages)

  val pipelin_model=pipeline.fit(train_df)
  val train_willFit_df=pipelin_model.transform(train_df).select("ID","target","features")
//          .withColumn("target",$"target"/10000d)
  val ChiSqSelectNums=train_willFit_df.select("features").take(1)(0).getAs[linalg.Vector](0).size
  val score=models.evaluateGDBT(train_willFit_df,"target")
  println(s"当前错误率上限：${fdr},已选择特征数量：$ChiSqSelectNums,score：${score}")
}

  def testChiSqByTopNum(train_df:DataFrame,num:Int)={
    val models=new Models(spark)
    val featureColumns_arr=train_df.columns.filterNot(column=>Constant.featureFilterColumns_arr.contains(column.toLowerCase))
    var stages:Array[PipelineStage]=FeatureUtils.vectorAssemble(featureColumns_arr,"assmbleFeatures")
    stages=stages:+  FeatureUtils.chiSqSelector("target","assmbleFeatures","features",num)
    val pipeline=new Pipeline().setStages(stages)

    val pipelin_model=pipeline.fit(train_df)
    val train_willFit_df=pipelin_model.transform(train_df).select("ID","target","features")
    //          .withColumn("target",$"target"/10000d)
    val ChiSqSelectNums=train_willFit_df.select("features").take(1)(0).getAs[linalg.Vector](0).size
    val score=models.evaluateGDBT(train_willFit_df,"target")
    println(s"当前特征数量：${num},已选择特征数量：$ChiSqSelectNums,score：${score}")
  }


  def fitByGBDT(train_df_source:DataFrame,test_df:DataFrame,fdr:Double,num:Int=1000)={
    val train_df=train_df_source.withColumn("target", log1p($"target"))

    val featureColumns_arr=train_df.columns.filterNot(column=>Constant.featureFilterColumns_arr.contains(column.toLowerCase))
    var stages:Array[PipelineStage]=FeatureUtils.vectorAssemble(featureColumns_arr,"assmbleFeatures")
//    stages=stages:+  FeatureUtils.chiSqSelectorByfdr("target","assmbleFeatures","features",fdr)
    stages=stages:+  FeatureUtils.chiSqSelector("target","assmbleFeatures","features",num)
    val pipeline=new Pipeline().setStages(stages)

    val pipelin_model = pipeline.fit(train_df)
    val train_willFit_df = pipelin_model.transform(train_df).select("ID", "target", "features")

    val test_willFit_df = pipelin_model.transform(test_df).select("id", "features")
    val models=new Models(spark)
    val lr_model = models.GBDT_TrainAndSave(train_willFit_df, "target")
    val format_udf = udf { prediction: Double =>
      "%08.9f".format(prediction)
    }
    val result_df = lr_model.transform(test_willFit_df)
//            .withColumn("target", format_udf(abs($"prediction" * 10000d)))
            .withColumn("target", format_udf(expm1(abs($"prediction"))))
            .select("id", "target")
    utils.writeToCSV(result_df, Constant.basePath + s"submission/lr_${System.currentTimeMillis()}")
  }
}
