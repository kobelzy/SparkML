package org.lzy.kaggle.kaggleSantander

import common.{FeatureUtils, Utils}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
/**
  * Created by Administrator on 2018/7/3.
  */
object FeatureExact {


}

class FeatureExact(spark: SparkSession) {
import spark.implicits._
  def selectFeaturesByRF(df: DataFrame,numOfFeatures:Int=100):Array[String] = {
    val featureColumns_arr=df.columns.filterNot(column=>Constant.featureFilterColumns_arr.contains(column.toLowerCase))
   val train=processFeatureBY_assemble_log1p(df)
    val Array(train_df, test_df) = train.randomSplit(Array(0.8, 0.2), seed = 10)
    val rf = new RandomForestRegressor().setSeed(7)
      .setLabelCol(Constant.lableCol)
    val rf_model=rf.fit(train_df)


    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol(Constant.lableCol)
    val rmse_score=evaluator.evaluate(rf_model.transform(test_df))

    println(s"随机森林特征选择验证RMSE分数位${rmse_score}")
//    spark.createDataFrame(Seq(rf_model.featureImportances,train.columns))
//    spark.create
    val feaImp_arr=rf_model.featureImportances.toArray
    val feaImp2Col_arr=feaImp_arr.zip(featureColumns_arr).sortBy(_._1).reverse.take(numOfFeatures)
    for((imp,col)<-feaImp2Col_arr){
      println(s"特征：${col},分数：$imp")    }

    feaImp2Col_arr.map(_._2)
  }

  def processFeatureBY_assemble_log1p(train_df: DataFrame)={
//    val train_df = df.withColumn("target", log1p($"target"))
    val featureColumns_arr=train_df.columns.filterNot(column=>Constant.featureFilterColumns_arr.contains(column.toLowerCase))
    var stages:Array[PipelineStage]=FeatureUtils.vectorAssemble(featureColumns_arr,"features")

    val pipeline=new Pipeline().setStages(stages)

    val pipelin_model=pipeline.fit(train_df)
    val train_willFit_df=pipelin_model.transform(train_df).select("ID",Constant.lableCol,"features")
    train_willFit_df
  }

}
