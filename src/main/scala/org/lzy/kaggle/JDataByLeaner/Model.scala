package org.lzy.kaggle.JDataByLeaner

import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostModel}
import org.apache.spark.internal.config
import org.apache.spark.{internal, ml}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Administrator on 2018/6/5.
  */
object Model {
  val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
  case class datas(features:ml.linalg.Vector, label:Double)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
//      .master("local[*]")
      .appName("model").getOrCreate()
    val config=spark.sparkContext.getConf
    config.set("spark.debug.maxToStringFields","100")
    config.set("spark.shuffle.io.maxRetries","60")
    config.set("spark.default.parallelism","54")
import spark.implicits._
val data:DataFrame =MLUtils.loadLibSVMFile(spark.sparkContext,basePath+"linear_regression_data.txt")
        .map(line=>datas(line.features.asML,line.label)).toDF()
    data.show(false)
    val Array(train,test)=data.randomSplit(Array(0.8,0.2))
    val result =featModel(train,test)
    result .show(false)
  }
def featModel(train_df:DataFrame,test_df:DataFrame)={
  //迭代次数
  val round=50
  //
  val nWorkers=10
  val useExternalMemory=true
val params=Map(
  "booster"->"gbtree",
  "objection"->"reg:linear",
  "eval_metric"->"rmse",
  "max_depth"->4,
  "eta"->0.05,
  "colsample_bytree"->0.9,
  "subsample"->0.8,
  "verbose_eval"->0
)
  val model:XGBoostModel=XGBoost.trainWithDataFrame(train_df,params,round,nWorkers)
  val result:DataFrame=model.transform(test_df)
   result
}
}
