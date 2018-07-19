package org.lzy.kaggle.kaggleSantander

import common.{FeatureUtils, Utils}
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.regression.{GBTRegressor, RandomForestRegressor}
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
  * Created by Administrator on 2018/7/3.
  */
object FeatureExact {
    def main(args: Array[String]): Unit = {
        val ks = new KolmogorovSmirnovTest()

        val ks_value = ks.kolmogorovSmirnovTest(Array(0.1, 0.2), Array(0.2, 0.3))
        println(ks_value)

//        val testResult = Statistics.kolmogorovSmirnovTest(data, "norm", 0, 1)
//        Statistics.

    }

}

class FeatureExact(spark: SparkSession) {

    import spark.implicits._

    /** *
      * 功能实现:通过随机森林计算选择特征
      *
      * Author: Lzy
      * Date: 2018/7/9 9:13
      * Param: [df, numOfFeatures]
      * Return: java.lang.String[]  经过排序的特征名，前numOfFeatures个
      */
    def selectFeaturesByRF(df: DataFrame, numOfFeatures: Int = 100): Array[String] = {
        val featureColumns_arr = df.columns.filterNot(column => Constant.featureFilterColumns_arr.contains(column.toLowerCase))
        val train = processFeatureBY_assemble_log1p(df)
        val Array(train_df, test_df) = train.randomSplit(Array(0.8, 0.2), seed = 10)
        val rf = new RandomForestRegressor().setSeed(10)
                .setLabelCol(Constant.lableCol)
        val rf_model = rf.fit(train_df)


        val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol(Constant.lableCol)
        val rmse_score = evaluator.evaluate(rf_model.transform(test_df))

        println(s"随机森林特征选择验证RMSE分数${rmse_score}")
        //    spark.createDataFrame(Seq(rf_model.featureImportances,train.columns))
//    spark.create
val feaImp_arr = rf_model.featureImportances.toArray
        val feaImp2Col_arr = feaImp_arr.zip(featureColumns_arr).sortBy(_._1).reverse.take(numOfFeatures)
        for ((imp, col) <- feaImp2Col_arr) {
            println(s"特征：${col},分数：$imp")
        }

        feaImp2Col_arr.map(_._2)
    }

    /** *
      * 功能实现:通过GBDT进行特征选择
      *
      * Author: Lzy
      * Date: 2018/7/9 14:55
      * Param: [df, numOfFeatures]
      * Return: java.lang.String[]
      */
    def selectFeaturesByGBDT(df: DataFrame, numOfFeatures: Int = 100): Array[String] = {
        val featureColumns_arr = df.columns.filterNot(column => Constant.featureFilterColumns_arr.contains(column.toLowerCase))
        val train = processFeatureBY_assemble_log1p(df)
        val Array(train_df, test_df) = train.randomSplit(Array(0.8, 0.2), seed = 10)
        val rf = new GBTRegressor().setSeed(10)
                .setLabelCol(Constant.lableCol)
                .setMaxIter(100)
        val rf_model = rf.fit(train_df)

        val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol(Constant.lableCol)
        val rmse_score = evaluator.evaluate(rf_model.transform(test_df))

        println(s"随机森林特征选择验证RMSE分数${rmse_score}")
        //    spark.createDataFrame(Seq(rf_model.featureImportances,train.columns))
//    spark.create
val feaImp_arr = rf_model.featureImportances.toArray
        val feaImp2Col_arr = feaImp_arr.zip(featureColumns_arr).sortBy(_._1).reverse.take(numOfFeatures)
        for ((imp, col) <- feaImp2Col_arr) {
            println(s"特征：${col},分数：$imp")
        }

        feaImp2Col_arr.map(_._2)
    }

    /** *
      * 功能实现:将传入的df进行log1p+根据指定的特征集记性assamble操作，
      *
      * Author: Lzy
      * Date: 2018/7/9 9:14
      * Param: [train_df]  包含了id，label和features
      * Return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
      */
    def processFeatureBY_assemble_log1p(train_df: DataFrame) = {
        //    val train_df = df.withColumn("target", log1p($"target"))
        val featureColumns_arr = train_df.columns.filterNot(column => Constant.featureFilterColumns_arr.contains(column.toLowerCase))
        var stages: Array[PipelineStage] = FeatureUtils.vectorAssemble(featureColumns_arr, "features")

        val pipeline = new Pipeline().setStages(stages)

        val pipelin_model = pipeline.fit(train_df)
        val train_willFit_df = pipelin_model.transform(train_df).select("ID", Constant.lableCol, "features")
        train_willFit_df
    }

    /** *  增加k个pca的聚合，并将其增加到df后边，返回features
      * 功能实现:
      *
      * Author: Lzy
      * Date: 2018/7/9 19:17
      * Param: [df, k, inputCol, outputCol]
      * Return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
      */
    def joinWithPCA(df: DataFrame, k: Int, inputCol: String, outputCol: String) = {
        var stages = Array[PipelineStage]()
        stages = stages :+ FeatureUtils.pca(k, inputCol, "pca")
        stages = stages ++ FeatureUtils.vectorAssemble(Array(inputCol, "pca"), outputCol)
        val pip = new Pipeline().setStages(stages)
        pip.fit(df).transform(df)
//                .select("id", Constant.lableCol,outputCol)
    }
/***
 * 功能实现:将数据分桶
 *zero:0.0   low:1.0   hight :2.0  top:3.0
 * Author: Lzy
 * Date: 2018/7/10 18:46
 * Param: [df, inputCol, outputCol]
 * Return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
 */
    def bucketFeatures(inputCol:String)={

        val splits=Array(Double.NegativeInfinity,0.1,math.pow(10,5),math.pow(10,6),math.pow(10,7),Double.PositiveInfinity)
        val bucketizer=new Bucketizer()
                .setInputCol(inputCol)
                .setOutputCol(inputCol+"_bucket")
                .setSplits(splits)
                .setHandleInvalid("skip")
        bucketizer
    }
    val bucketizer_udf=udf{column:Double=>
        if(column <=0) 0d
        else if(column >0 && column <=math.pow(10,5)) 1d
        else if(column >math.pow(10,5) && column <=math.pow(10,6)) 2d
        else if(column >math.pow(10,6) && column <=math.pow(10,7)) 3d
        else 4d
    }
    /***
     * 功能实现:将制定列进行分桶，并将分桶后的记过进行assmble为outputCol名称
     *
     * Author: Lzy
     * Date: 2018/7/10 18:59
     * Param: [df, columns]
     * Return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     */
    def featureBucketzer(df:DataFrame,columns:Array[String],outputCol:String)={
        val assambleColumns=columns.take(2)
                .map(_+"_bucket")
        var stages= Array[PipelineStage]()
//var buckt_df=df
        columns.take(2).foreach(column=>{
            stages=stages:+bucketFeatures(column)
//            buckt_df=buckt_df.withColumn(column,bucketizer_udf(col(column)))
        })
        stages=stages++(FeatureUtils vectorAssemble(assambleColumns, outputCol))
        val pipline=new Pipeline().setStages(stages)

        pipline.fit(df).transform(df)
    }

    def addStatitic(df:DataFrame)={
        val columns=df.columns.filterNot(column=>(Constant.featureFilterColumns_arr:+"df_type").contains(column.toLowerCase()))
        val column_count=columns.length
        val median_index=(column_count/2.0).toInt

        val df_rdd=df.select((col("id")+:(columns.map(column=>col(column).cast(DoubleType)))):_*).rdd

          .map(row=>{
              var arr=Array[Double]()
              for(i <- columns.indices){
                  arr=arr:+row.getDouble(i+1)
              }
              val id=row.getString(0)
              (id,arr)
          })
        val statistic_df=df_rdd.map { case (id, arr) => {
            val sum = arr.sum
            val mean=sum/column_count
            val std=math.sqrt(arr.map(x=>math.pow(x-mean,2)).sum)
            val nans=arr.count(x => x == 0 || x == 0d)
            val sort_arr= arr.sorted
            val median=sort_arr(median_index)
            (id,sum,mean,std,nans,median)
        }
        }.toDF("id","sum","mean","std","nans","median")
//        statistic_df.coalesce(10).write.mode(SaveMode.Overwrite).parquet(Constant.basePath+"cache/statistic_df")
        statistic_df.coalesce(10).write.mode(SaveMode.Overwrite).parquet(Constant.basePath+"cache/evaluate_statistic_df")
        statistic_df.show()
        statistic_df
        }


}
