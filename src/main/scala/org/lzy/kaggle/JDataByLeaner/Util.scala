package org.lzy.kaggle.JDataByLeaner

import java.sql.Timestamp

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, countDistinct, quarter}
import org.apache.spark.sql.functions._
/**
  * Created by Administrator on 2018/5/30.
  */

object Util {
    def main(args: Array[String]): Unit = {
        val basePath = "E:\\dataset\\JData_UserShop\\"

        val spark = SparkSession.builder().appName("names")
                .master("local[*]")
                .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        val util = new Util(spark)
        val order = util.getSourceData(basePath + "jdata_user_order_test.csv")
        val user = util.getSourceData(basePath + "jdata_user_basic_info_test.csv").cache()
        //测试方法loadData
        //    val (order, action) = util.loadData(basePath)
        //    order.show()
        //    action.show()

        //测试方法featUnique
        featCount(user, order, Array("user_id"), "o_id",Some("index")).show(false)
        featCount2(user, order, Array("user_id"), "o_id",Some("index")).show(false)

//        feaStd(user, order, Array("user_id"), "o_id",Some("index")).show(false)
//        featMean(user, order, Array("user_id"), "o_id",Some("index")).show(false)
//        featMax(user, order, Array("user_id"), "o_id",Some("index")).show(false)
//        featMin(user, order, Array("user_id"), "o_id",Some("index")).show(false)
//        featSum(user, order, Array("user_id"), "o_id",Some("index")).show(false)
//        featUnique(user, order, Array("user_id"), "o_id",Some("index")).show(false)
    }



    def featCount(df: DataFrame, df_feature: DataFrame, fe: Array[String], value: String, name: Option[String] = None) = {
        val newName = name.getOrElse(value + fe.mkString("_", "_", "_") + "count")
        val df_count: DataFrame = df_feature.groupBy(fe.head, fe.tail: _*).count().select("count", fe: _*)
                .withColumnRenamed("count", newName)
        df.join(df_count, fe, "left").na.fill(0)
    }

    def featCount2(df: DataFrame, df_feature: DataFrame, fe: Array[String], value: String, name: Option[String] = None) = {
        val newName = name.getOrElse(value + fe.mkString("_", "_", "_") + "count")
        val df_count: DataFrame = df_feature.groupBy(fe.head, fe.tail: _*).agg(count(value).as(newName)).select(newName, fe: _*)
        df.join(df_count, fe, "left").na.fill(0)
    }
    /***
     * 功能实现:标准差
     *
     * Author: Lzy
     * Date: 2018/6/1 9:31
     * Param: [df, df_feature, fe, value, name]
     * Return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     */
    def feaStd(df: DataFrame, df_feature: DataFrame, fe: Array[String], value: String, name: Option[String] = None) = {
        val newName = name.getOrElse(value + fe.mkString("_", "_", "_") + "std")
        val df_count: DataFrame = df_feature.groupBy(fe.head, fe.tail: _*).agg(stddev(value).as(newName)).select(newName, fe: _*)
        df.join(df_count, fe, "left").na.fill(0)

    }
    def featMean(df: DataFrame, df_feature: DataFrame, fe: Array[String], value: String, name: Option[String] = None) = {
        val newName = name.getOrElse(value + fe.mkString("_", "_", "_") + "mean")
        val df_count: DataFrame = df_feature.groupBy(fe.head, fe.tail: _*).agg(mean(value).as(newName)).select(newName, fe: _*)
        df.join(df_count, fe, "left").na.fill(0)

    }
    /***
     * 功能实现:非NA的算数中位数
     *
     * Author: Lzy
     * Date: 2018/6/1 9:32
     * Param: [df, df_feature, fe, value, name]
     * Return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     */
//    def featMedian(df: DataFrame, df_feature: DataFrame, fe: Array[String], value: String, name: Option[String] = None) = {
//        val newName = name.getOrElse(value + fe.mkString("_", "_", "_") + "nunique")
//        val df_count: DataFrame = df_feature.groupBy(fe.head, fe.tail: _*).agg(.select("std", fe: _*)
//                .withColumnRenamed("std", newName)
//        df_feature
//    }
    def featMax(df: DataFrame, df_feature: DataFrame, fe: Array[String], value: String, name: Option[String] = None) = {
        val newName = name.getOrElse(value + fe.mkString("_", "_", "_") + "max")
        val df_count: DataFrame = df_feature.groupBy(fe.head, fe.tail: _*).agg(max(value).as(newName)).select(newName, fe: _*)
    df.join(df_count, fe, "left").na.fill(0)

}
    def featMin(df: DataFrame, df_feature: DataFrame, fe: Array[String], value: String, name: Option[String] = None) = {
        val newName = name.getOrElse(value + fe.mkString("_", "_", "_") + "min")
        val df_count: DataFrame = df_feature.groupBy(fe.head, fe.tail: _*).agg(min(value).as(newName)).select(newName ,fe: _*)
        df.join(df_count, fe, "left").na.fill(0)

    }
    def featSum(df: DataFrame, df_feature: DataFrame, fe: Array[String], value: String, name: Option[String] = None) = {
        val newName = name.getOrElse(value + fe.mkString("_", "_", "_") + "sum")
        val df_count: DataFrame = df_feature.groupBy(fe.head, fe.tail: _*).agg(sum(value).as(newName)).select(newName, fe: _*)
        df.join(df_count, fe, "left").na.fill(0)

    }
    /***
     * 功能实现:方差
     *
     * Author: Lzy
     * Date: 2018/6/1 9:31
     * Param: [df, df_feature, fe, value, name]
     * Return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     */
    def feaVar(df: DataFrame, df_feature: DataFrame, fe: Array[String], value: String, name: Option[String] = None) = {
        val newName = name.getOrElse(value + fe.mkString("_", "_", "_") + "var")
        val df_count: DataFrame = df_feature.groupBy(fe.head, fe.tail: _*).agg(variance(value).as(newName)).select(newName, fe: _*)
        df.join(df_count, fe, "left").na.fill(0)
    }
    /** *
      * 功能实现:
      * 特征种类，用于将df_feature的fe分组之后，选择每一组中的value字段去重后的数量放到一个新的字段中，该字段名称由name或者value来决定
      * 之后将该df_feature与df进行join，获得根据fe的join结果
      * 主要就是希望获得df中对应fe字段在字典表中的类型个数。
      * Author: Lzy
      * Date: 2018/6/1 9:09
      * Param: [df, df_feature, fe, value, name]
      * Return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
      */
    def featUnique(df: DataFrame, df_feature: DataFrame, fe: Seq[String], value: String, name: Option[String] = None) = {
        val newName = name.getOrElse(value + fe.mkString("_", "_", "_") + "nunique")
        val df_count: DataFrame = df_feature.groupBy(fe.head, fe.tail: _*).agg(countDistinct(value).as(newName))
          .select(newName, fe: _*)
//                .withColumnRenamed("count", newName)
        df.join(df_count, fe, "left").na.fill(0)
    }


    //########################################################
    def mergeCount(df:DataFrame,columns:Array[String],value:String,cname:String)={
        val add=df.groupBy(columns.head,columns.tail:_*).agg(count(value).as(cname)).select(cname,columns:_*)
        df.join(add,columns,"left")
    }

    def mergeNunique(df:DataFrame,columns:Array[String],value:String,cname:String)={
        val add=df.groupBy(columns.head,columns.tail:_*).agg(countDistinct(value).as(cname)).select(cname,columns:_*)
        df.join(add,columns,"left")
    }

    def mergeMin(df:DataFrame,columns:Array[String],value:String,cname:String)={
        val add=df.groupBy(columns.head,columns.tail:_*).agg(min(value).as(cname)).select(cname,columns:_*)
        df.join(add,columns,"left")
    }

    def mergeMax(df:DataFrame,columns:Array[String],value:String,cname:String)={
        val add=df.groupBy(columns.head,columns.tail:_*).agg(max(value).as(cname)).select(cname,columns:_*)
        df.join(add,columns,"left")
    }

    def mergeSum(df:DataFrame,columns:Array[String],value:String,cname:String)={
        val add=df.groupBy(columns.head,columns.tail:_*).agg(sum(value).as(cname)).select(cname,columns:_*)
        df.join(add,columns,"left")
    }

    def mergeStd(df:DataFrame,columns:Array[String],value:String,cname:String)={
        val add=df.groupBy(columns.head,columns.tail:_*).agg(stddev(value).as(cname)).select(cname,columns:_*)
        df.join(add,columns,"left")
    }

    def mergeMean(df:DataFrame,columns:Array[String],value:String,cname:String)={
        val add=df.groupBy(columns.head,columns.tail:_*).agg(mean(value).as(cname)).select(cname,columns:_*)
        df.join(add,columns,"left")
    }

//    def mergeMedian(df:DataFrame,columns:Array[String],value:String,cname:String)={
//        val add=df.groupBy(columns.head,columns.tail:_*).agg(countDistinct(value).as(cname)).select(cname,columns:_*)
//        df.join(add,columns,"left")
//    }


    //#####################################################
    def encodeCount(df:DataFrame,ColumnName:String)={

    }

    def stageByOneHot(column: String): Array[PipelineStage] = {
        var stages = Array[PipelineStage]()
        val string2vector = new StringIndexer()
          .setInputCol(column)
          .setOutputCol(column + "_string2vector")
        stages = stages :+ string2vector
        val onehoter: OneHotEncoder = new OneHotEncoder()
          .setInputCol(column + "_string2vector").setOutputCol(column + "_onehot")
        stages = stages :+ onehoter
        stages
    }
}

class Util(spark: SparkSession) {
import spark.implicits._
    /**
      * 获取csv转换为DF
      *
      * @param path
      * @return
      */
    def getSourceData(path: String): DataFrame = {
        val data = spark.read.option("header", "true")
                .option("nullValue", "NA")
                .option("inferSchema", "true")
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .csv(path)
        data
    }

    /** *
      * 功能实现:
      * 将订单表联合用户、商品、评价并提取年月日输出
      * 将行为表联合用户、商品并提取年月日输出
      * Author: Lzy
      * Date: 2018/6/1 9:13
      * Param: [basePath]
      * Return: scala.Tuple2<org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>,org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>>
      */
    def loadData(basePath: String = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"): (DataFrame, DataFrame) = {
        val sku = "jdata_sku_basic_info.csv"
        val user_basic = "jdata_user_basic_info.csv"
        val user_action = "jdata_user_action.csv"
        val user_order = "jdata_user_order.csv"
        val user_comment = "jdata_user_comment_score.csv"

        val sku_df = getSourceData(basePath + sku)
        //用户信息,user_id,age,sex,user_lv_cd
        val user_df = getSourceData(basePath + user_basic)
        //用户行为，user_id,sku_id,a_date,a_num,a_type
        val action_df = getSourceData(basePath + user_action)
        //订单表，user_id,sku_id,o_id,o_date,o_area,o_sku_num
        val order_df = getSourceData(basePath + user_order)
                .cache()
        //评价表,user_id,comment_create_tm,o_id,score_level
        val comment_df = getSourceData(basePath + user_comment)

        val user_action_df = action_df.withColumn("a_year", year(action_df("a_date")))
                .withColumn("a_month",month($"a_date"))
                .withColumn("a_day", dayofmonth($"a_date"))

        val user_order_df = order_df.withColumn("o_year", year(order_df("o_date")))
                .withColumn("o_month", month(order_df("o_date")))
                .withColumn("o_day", dayofmonth(order_df("o_date")))

        val user_comment_df = comment_df
                .withColumn("c_year", year(comment_df("comment_create_tm")))
                .withColumn("c_month", month(comment_df("comment_create_tm")))
                .withColumn("c_day", dayofmonth(comment_df("comment_create_tm")))
                .withColumnRenamed("comment_create_tm", "c_date")

        //    * 把user_order,user_comment,sku,user_info连在一起组成order_comment表
        val order = user_order_df.join(broadcast(sku_df), Seq("sku_id"), "left")
                .join(broadcast(user_df), Seq("user_id"), "left")
                .join(broadcast(user_comment_df), Seq("user_id", "o_id"), "left")

        //把user_action,user_info,sku_info连接一起组成user_action表
        val action = user_action_df.join(broadcast(sku_df), Seq("sku_id"), "left")
                .join(broadcast(user_df), Seq("user_id"), "left")
        (order, action)
    }

    val getMonthFromTime = udf { (time: Timestamp) => time.toLocalDateTime.getMonthValue }
    val getDayFromTime = udf { (time: Timestamp) => time.toLocalDateTime.getDayOfMonth }
    val getYearFromTime = udf { (time: Timestamp) => time.toLocalDateTime.getYear }


}

