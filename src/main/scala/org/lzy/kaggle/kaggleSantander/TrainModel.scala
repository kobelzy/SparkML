package org.lzy.kaggle.kaggleSantander

import common.{FeatureUtils, Utils}
import org.apache.spark.ml.classification.GBTClassificationModel
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.ml.tuning.TrainValidationSplitModel
import org.apache.spark.ml.{Pipeline, PipelineStage, linalg}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by Administrator on 2018/7/3.
  */
object TrainModel {
  def main(args: Array[String]): Unit = {

  }


}

class TrainModel(spark: SparkSession) {

  import spark.implicits._


  /** *
    * 功能实现:使用卡方检验进行特征选择
    *
    * Author: Lzy
    * Date: 2018/7/9 9:20
    * Param: [train_df_source, fdr]
    * Return: void
    */
  def testSelectorChiSq(train_df_source: DataFrame, type_num: Int = 0, arg: Double) = {
    //    1、最大数量，0错误率上限
    val models = new Models(spark)

    val train_df = train_df_source.withColumn("target", log1p($"target"))

    val featureColumns_arr = train_df.columns.filterNot(column => Constant.featureFilterColumns_arr.contains(column.toLowerCase))
    var stages: Array[PipelineStage] = FeatureUtils.vectorAssemble(featureColumns_arr, "assmbleFeatures")
    val chiSqSelector_pipstage: PipelineStage = type_num match {
      case 0 => FeatureUtils.chiSqSelectorByfdr("target", "assmbleFeatures", "features", arg)
      case 1 => FeatureUtils.chiSqSelector("target", "assmbleFeatures", "features", arg.toInt)
    }
    stages = stages :+ chiSqSelector_pipstage

    val pipeline = new Pipeline().setStages(stages)
    val pipelin_model = pipeline.fit(train_df)
    val train_willFit_df = pipelin_model.transform(train_df).select("ID", "target", "features")
    //          .withColumn("target",$"target"/10000d)
    val ChiSqSelectNums = train_willFit_df.select("features").take(1)(0).getAs[linalg.Vector](0).size
    val score = models.evaluateGDBT(train_willFit_df, "target")
    println(s"当前错误率上限/特征数量：${arg},已选择特征数量：$ChiSqSelectNums,score：${score}")
  }

  /** *
    * 功能实现:使用随机森林进行特征 选择测试
    *
    * Author: Lzy
    * Date: 2018/7/9 9:30
    * Param: [train_df_source, num]
    * Return: void
    */
  def testSelectorRF(train_df_source: DataFrame, num: Int) = {
    val models = new Models(spark)
    val featureExact = new FeatureExact(spark)

    val train_df = train_df_source.withColumn("target", log1p($"target"))

    val featureColumns_arr = featureExact.selectFeaturesByRF(train_df, num)
    var stages: Array[PipelineStage] = FeatureUtils.vectorAssemble(featureColumns_arr, "features")
    val pipeline = new Pipeline().setStages(stages)

    val pipelin_model = pipeline.fit(train_df)
    val train_willFit_df = pipelin_model.transform(train_df).select("ID", "target", "features")
    //          .withColumn("target",$"target"/10000d)
    val ChiSqSelectNums = train_willFit_df.select("features").take(1)(0).getAs[linalg.Vector](0).size
    val score = models.evaluateGDBT(train_willFit_df, "target")
    println(s"当前特征数量：${num},已选择特征数量：$ChiSqSelectNums,score：${score}")
  }

  def testSelectorGBDT(train_df_source: DataFrame, num: Int) = {
    val models = new Models(spark)
    val featureExact = new FeatureExact(spark)

    val train_df = train_df_source.withColumn("target", log1p($"target"))

    val featureColumns_arr = featureExact.selectFeaturesByGBDT(train_df, num)
    var stages: Array[PipelineStage] = FeatureUtils.vectorAssemble(featureColumns_arr, "features")
    val pipeline = new Pipeline().setStages(stages)

    val pipelin_model = pipeline.fit(train_df)
    val train_willFit_df = pipelin_model.transform(train_df).select("ID", "target", "features")
    //          .withColumn("target",$"target"/10000d)
    val ChiSqSelectNums = train_willFit_df.select("features").take(1)(0).getAs[linalg.Vector](0).size
    val score = models.evaluateGDBT(train_willFit_df, "target")
    println(s"当前特征数量：${num},已选择特征数量：$ChiSqSelectNums,score：${score}")
  }

  /** *
    * 功能实现:使用GBDT进行训练模型，并导出结果数据
    *
    * Author: Lzy
    * Date: 2018/7/9 9:30
    * Param: [train_df_source, test_df, fdr, num]
    * Return: void
    */
  def fitByGBDT(train_df_source: DataFrame, test_df: DataFrame, fdr: Double, num: Int = 1000) = {
    val models = new Models(spark)
    val featureExact = new FeatureExact(spark)
    val utils = new Utils(spark)

    //        val train_df = train_df_source.withColumn("target",  log1p($"target"))
    val train_df = train_df_source.withColumn("target", round(log1p($"target")))

    //        val featureColumns_arr = featureExact.selectFeaturesByRF(train_df, num)
    val featureColumns_arr = Constant.specialColumns_arr
    //        val featureColumns_arr = featureExact.selectFeaturesByGBDT(train_df, num)
    var stages: Array[PipelineStage] = FeatureUtils.vectorAssemble(featureColumns_arr, "features")

    //    stages=stages:+  FeatureUtils.chiSqSelectorByfdr("target","assmbleFeatures","features",fdr)
    //        stages = stages :+ FeatureUtils.chiSqSelector("target", "assmbleFeatures", "features", num)
    val pipeline = new Pipeline().setStages(stages)

    val pipelin_model = pipeline.fit(train_df)
    //        val train_willFit_df = pipelin_model.transform(train_df).select("ID", "target", "features")
    //增加pca100
    val train_willFitToPCA_df = pipelin_model.transform(train_df).select("ID", "features", "target").withColumn("type", lit(0))
    val test_willFitToPCA_df = pipelin_model.transform(test_df).select("ID", "features").withColumn(Constant.lableCol, lit(0d)).withColumn("type", lit(1))

    val tmp_df = train_willFitToPCA_df.union(test_willFitToPCA_df)

    //        val tmp_pca_df = featureExact.joinWithPCA(tmp_df, 100, "assmbleFeatures", "features")
    val train_willFit_df = tmp_df.filter($"type" === 0).select("ID", "target", "features")
    val test_willFit_df = tmp_df.filter($"type" === 1).select("ID", "features")

    val lr_model = models.GBDT_TrainAndSave(train_willFit_df, "target")
    //        val lr_model = models.RFClassic_TrainAndSave(train_willFit_df, "target")
    //        val lr_model=XGBoostModel.fitPredictByLogistic(train_willFit_df,"target","features",100)

    val format_udf = udf { prediction: Double =>
      "%08.9f".format(prediction)
    }
    val result_df = lr_model.transform(test_willFit_df)
      //            .withColumn("target", format_udf(abs($"prediction" * 10000d)))
      .withColumn("target", format_udf(expm1(abs($"prediction"))))
      .select("id", "target")
    utils.writeToCSV(result_df, Constant.basePath + s"submission/lr_${System.currentTimeMillis()}")
  }

  /** *
    * 功能实现:
    * 使用分桶来划分数据，并将结果按照log1p进行四舍五入，做分类。
    * Author: Lzy
    * Date: 2018/7/10 19:25
    * Param: [train_df_source, ColumnNum]
    * Return: org.apache.spark.ml.classification.GBTClassificationModel
    */
  def fitByGBDTAndBucket(all_df_source: DataFrame, ColumnNum: Int = 1000): Unit = {
    val models = new Models(spark)
    val featureExact = new FeatureExact(spark)

    val all_df = all_df_source.withColumn("target", round(log1p($"target")))

    val featureColumns_arr = all_df.columns.filterNot(column => Constant.featureFilterColumns_arr.contains(column.toLowerCase))

    val all_feaPro_df = featureExact.featureBucketzer(all_df, featureColumns_arr, "features")
    all_feaPro_df.write.mode(SaveMode.Overwrite).parquet(Constant.basePath + "cache/all_bucket_df")
    val (train_df, test_df) = FeatureUtils.splitTrainAndTest(all_feaPro_df)
    val train = train_df.select("id", "target", "features")
    val test = test_df.select("id", "features")
    //        val gbdt_model = models.RFClassic_TrainAndSave(train, Constant.lableCol, "features")
    //        val result_df=gbdt_model.transform(test)
    //        writeSub(result_df)
    test.show(false)
  }

  def transformAndExplot_GBDTBucket(test_df: DataFrame, modelPath: String) = {
    val result_df = GBTClassificationModel.load(modelPath).transform(test_df)
    writeSub(result_df)

  }

  /** *
    * 功能实现:加载GBDT模型，并训练结果文件，
    *
    * Author: Lzy
    * Date: 2018/7/9 9:36
    * Param: [test_df, modelPath]
    * Return: void
    */
  def transformAndExplot_GBDT(test_df: DataFrame, modelPath: String) = {
    val model = GBTRegressionModel.load(modelPath)
    val result_df = model.transform(test_df)
    writeSub(result_df)
  }


  /** *
    * 功能实现:加载GBDT模型，并训练结果文件，
    *
    * Author: Lzy
    * Date: 2018/7/9 9:36
    * Param: [test_df, modelPath]
    * Return: void
    */
  def transformAndExplot_TV(test_df: DataFrame, modelPath: String) = {
    val model = TrainValidationSplitModel.load(modelPath)
    val result_df = model.transform(test_df)
    writeSub(result_df)

  }

  /** *
    * 功能实现:将数据写出到结果文件
    *
    * Author: Lzy
    * Date: 2018/7/10 19:24
    * Param: [df]
    * Return: void
    */
  def writeSub(df: DataFrame) = {
    val utils = new Utils(spark)
    val format_udf = udf { prediction: Double =>
      "%08.9f".format(prediction)
    }
    val result_df = df.withColumn("target", format_udf(expm1(abs($"prediction"))))
      .select("id", Constant.lableCol)
    val subName = s"sub_${System.currentTimeMillis()}"
    println(s"当前结果文件：${subName}")
    utils.writeToCSV(result_df, Constant.basePath + s"submission/$subName")
    println("action:?" + result_df.count())
  }


  def generateFromColumns(test: DataFrame) = {
    //        Array("f190486d6","58e2e02e6","eeb9cd3aa","9fd594eec","6eef030c1","58e056e12")
    val columns = Constant.specialColumns_arr
    val result = test.select($"id", ($"f190486d6" + $"58e2e02e6" + $"eeb9cd3aa" + $"9fd594eec" + $"6eef030c1" + $"58e056e12").alias("result")).select($"id", ($"result" / 6).alias("prediction"))
    writeSub(result)
  }

  def fitWithStatistic(train_df_source: DataFrame, test: DataFrame): Unit = {
    val train = train_df_source.withColumn("target", round(log1p($"target")))
    val featureExact = new FeatureExact(spark)
    val model = new Models(spark)

    val featureColumns_arr = Constant.specialColumns_arr ++ Array("sum", "mean", "std", "nans", "median")
    val all_df = FeatureUtils.concatTrainAndTest(train, test, Constant.lableCol)
    val statistic_df =
//      featureExact.addStatitic(all_df)
            spark.read.parquet(Constant.basePath+"cache/statistic_df")
      val all_epecial_df:Array[String]=Constant.specialColumns_arr:+"target":+"df_type"
    val all_statistic_df = all_df.select("id",all_epecial_df:_*) .join(broadcast(statistic_df), "id")
    statistic_df.show()


    all_statistic_df.show()
    val stages: Array[PipelineStage] = FeatureUtils.vectorAssemble(featureColumns_arr, "features")
    val pipeline = new Pipeline().setStages(stages)

    val pipelin_model = pipeline.fit(all_statistic_df)
    val all_transfer_df = pipelin_model.transform(all_statistic_df)
    val (train_df, test_df) = FeatureUtils.splitTrainAndTest(all_transfer_df)

    val gbdtModel: GBTRegressionModel = model.GBDT_TrainAndSave(train_df, Constant.lableCol)
    val result_df = gbdtModel.transform(test_df)
    writeSub(result_df)
  }


  def evaluateWithStatistic(train_df_source: DataFrame, statistic_columns: Array[String] = Array("sum", "mean", "std", "nans", "median")) = {
    val featureExact = new FeatureExact(spark)
    val models = new Models(spark)
    val all_df = train_df_source.withColumn("target", round(log1p($"target")))



    val featureColumns_arr = Constant.specialColumns_arr ++ statistic_columns
    val statistic_df =
// featureExact.addStatitic(all_df)
            spark.read.parquet(Constant.basePath+"cache/evaluate_statistic_df")

    val all_epecial_df:Array[String]=Constant.specialColumns_arr:+"target"

    val all_statistic_df = all_df.select("id",all_epecial_df:_*) .join(broadcast(statistic_df), "id").cache()
    all_statistic_df.checkpoint()
    val stages: Array[PipelineStage] = FeatureUtils.vectorAssemble(featureColumns_arr, "features")
    val pipeline = new Pipeline().setStages(stages)

    val pipelin_model = pipeline.fit(all_statistic_df)
    val all_transfer_df = pipelin_model.transform(all_statistic_df)

    val model = new Models(spark)
    val gbdtModel: GBTRegressionModel = model.GBDT_TrainAndSave(all_transfer_df, Constant.lableCol)
    val train_willFit_df = gbdtModel.transform(all_transfer_df).select("ID", "target", "features")
    //          .withColumn("target",$"target"/10000d)
    val score = models.evaluateGDBT(train_willFit_df, "target")
    println(s"当前统计特征：${statistic_columns.mkString(",")}....score：${score}")
  }


  def lagSelectFakeRows(train:DataFrame,test:DataFrame)={
    val featureExact=new FeatureExact(spark)
    val (trainLeak_df, leaky_train_counts, leaky_value_corrects,scores)=featureExact.compiledLeadResult(train)
    val bestScore=scores.min
    val best_lag=scores.indexOf(bestScore)
    println("最高分值："+bestScore+",下标："+best_lag)

    val (test_leak,leaky_test_counts)=featureExact.compiledLeakResult_test(test,38)

    val test_leak_df=featureExact.reWriteCompiledLeak(test_leak,best_lag)
    test_leak_df.select("id",Constant.specialColumns_arr++Array("compiled_leak","nonzero_mean"):_*).show()

//    val test_res=test_leak.select("compiled_leak",Constant.specialColumns_arr:_*)、、

    test_leak_df.filter($"compiled_leak"===0)
    val fill_test_leak_df=test_leak_df.withColumn("compiled_leak",when($"compiled_leak"===0,$"nonzero_mean"))

    test.withColumn("target",lit(0d))
  }
}
