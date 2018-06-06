package org.lzy.kaggle.JDataByLeaner

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.mutable
/**
  * Created by Administrator on 2018/6/3.
  */
object TrainModels{
  //  val basePath = "E:\\dataset\\JData_UserShop\\"
  val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
            .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val util = new Util(spark)
    val trainModel=new TrainModels(spark,basePath)
  }
}
class TrainModels(spark: SparkSession, basePath: String) {
import spark.implicits._


  def score(df:DataFrame)={
    val udf_getWeight=udf{index:Int=>
      1/(1+math.log(index+1))
    }

    val udf_binary=udf{label_1:Int=>if (label_1>0) 1 else 0}
    val weight_df=df.sort("o_num").withColumn("new_label",udf_binary($"label_1")).drop("label_1").withColumnRenamed("new_label","label_1")
      .limit(50000)
          .withColumn("index",monotonically_increasing_id)
      .withColumn("weight",udf_getWeight($"index"))
    val s1=weight_df.filter($"label_1" === $"weight").select($"label_1".as[Int]).collect().sum/4674.239
    val df_label_1=weight_df.filter($"label_1" ===1)
    val s2=weight_df.select($"label_2".as[Int],$"pred_date".as[Int]).collect().map{case (label_2,pred_date)=>
    10.0/(math.round(label_2)-pred_date*pred_date+10)
    }.sum /weight_df.count()
    println(s"s1 score is $s1 ,s2 score is $s2 , S is ${0.4 * s1 + 0.6 * s2}")
  }
def getTrain()={
val train=spark.read.parquet(basePath + "cache/vali_train")
  val test=spark.read.parquet(basePath+"cache/vali_test")
  val dropColumns=Array("user_id","label_1","label_2")
  val result=test.select("user_id","label_1","label_2")
  val X=train.drop(dropColumns:_*)
  val X_pred=test.drop(dropColumns:_*)
  val y=train.select("label_1")
  //为resul通过label_1来计算 添加o_num列，

  //为result通过label_2来计算添加pred_date
  //result包括了"user_id","label_1","label_2","o_num","pred_date"
  score(result)
   }
  def getResult()={

  }

  def fitPredict()={

  }
}
