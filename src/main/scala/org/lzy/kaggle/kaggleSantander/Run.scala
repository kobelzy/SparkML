package org.lzy.kaggle.kaggleSantander

import common.{FeatureUtils, Utils}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
/*
   spark-submit --master yarn-cluster --queue all --driver-memory 6g --conf spark.driver.maxResultSize=5g  \
   --num-executors 14 --executor-cores 4 --executor-memory 6g  \
   --class org.lzy.kaggle.kaggleSantander.Run SparkML.jar
 */
/**
  * Created by Administrator on 2018/7/3.
  *
  * Created by Administrator on 2018/6/3

  **/
object Run {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("names")
//            .master("local[*]")
                .getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("WARN")
        val conf = spark.conf
        val sc = spark.sparkContext
        val config = sc.getConf
        //    config.set("spark.driver.maxResultSize","0")
        config.set("spark.debug.maxToStringFields", "100")
        config.set("spark.shuffle.io.maxRetries", "60")
        config.set("spark.default.parallelism", "54")
        config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val utils = new Utils(spark)
        val models = new Models(spark)
        val featureExact = new FeatureExact(spark)
        val run = new Run(spark)
        val trainModel = new TrainModel(spark)
        val train_df = utils.readToCSV(Constant.basePath + "AData/train.csv").repartition(100).cache()
        val test_df = utils.readToCSV(Constant.basePath + "AData/test.csv").repartition(100).cache()
        /*
        验证GBDT在不同参数情况下的分数
         */
//        run.evaluatorGBDT(train_df)
//        trainModel.testChiSqByFdr(train_df, 0.01)
//        trainModel.testChiSqByRFSelect(train_df,372)
//        trainModel.testChiSqByTopNum(train_df,372)
        /*
        训练GBDT并将数据导出
         */
        trainModel.fitByGBDT(train_df, test_df, 0.01, 400)

        /*
        通过随机森林训练查看特征重要性。
         */
//        featureExact.selectFeaturesByRF(train_df)

    }
}

class Run(spark: SparkSession) {

    import spark.implicits._

    /** *
      * 功能实现:
      * 通过多个变量值来检测使用GBDT得到的分数
      * Author: Lzy
      * Date: 2018/7/9 9:19
      * Param: [train_df]
      * Return: void
      */
    def evaluatorGBDT(train_df: DataFrame) = {

        val trainModel = new TrainModel(spark)
//        Array(0.01, 0.001, 0.003, 0.005).foreach(fdr => {
//            trainModel.testChiSqByFdr(train_df, fdr)
//        })
        Array(500, 1000, 2000, 3000).foreach(num => {
            trainModel.testSelectorChiSq(train_df, type_num = 0, num)
        })
    }

    def run1 = {
        val spark = SparkSession.builder().appName("names")
                //            .master("local[*]")
                .getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("WARN")
        val conf = spark.conf
        val sc = spark.sparkContext
        val config = sc.getConf
        //    config.set("spark.driver.maxResultSize","0")
        config.set("spark.debug.maxToStringFields", "100")
        config.set("spark.shuffle.io.maxRetries", "60")
        config.set("spark.default.parallelism", "54")
        config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val utils = new Utils(spark)
        val models = new Models(spark)
        val train_df = utils.readToCSV(Constant.basePath + "AData/train.csv").repartition(100).cache()
        val test_df = utils.readToCSV(Constant.basePath + "AData/test.csv").repartition(100).cache()

        val featureFilterColumns_arr = Array("id", "target")
        val featureColumns_arr = train_df.columns.filterNot(column => featureFilterColumns_arr.contains(column.toLowerCase))
        var stages: Array[PipelineStage] = FeatureUtils.vectorAssemble(featureColumns_arr, "assmbleFeatures")
        stages = stages :+ FeatureUtils.chiSqSelector("target", "assmbleFeatures", "features", 1000)
        val pipeline = new Pipeline().setStages(stages)

        val pipelin_model = pipeline.fit(train_df)
        val train_willFit_df = pipelin_model.transform(train_df).select("ID", "target", "features").withColumn("target", $"target" / 10000d)
        val test_willFit_df = pipelin_model.transform(test_df).select("id", "features")

        val lr_model = models.GBDT_TrainAndSave(train_willFit_df, "target")
        val format_udf = udf { prediction: Double =>
            "%08.9f".format(prediction)
        }
        val result_df = lr_model.transform(test_willFit_df).withColumn("target", format_udf(abs($"prediction" * 10000d)))
                .select("id", "target")
        utils.writeToCSV(result_df, Constant.basePath + s"submission/lr_${System.currentTimeMillis()}")
    }
}
