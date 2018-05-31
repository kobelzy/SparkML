package org.lzy.kaggle.JDataByLeaner

import java.sql.Timestamp
import breeze.linalg.*
import com.ml.kaggle.JData.TimeFuture.basePath
import kingpoint.timeSeries.time
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col

/**
  * Created by Administrator on 2018/5/30.
  */

object Util{

  def featNunique(df:DataFrame,df_feature:DataFrame,fe:Seq[String],value:String,name:String="")={
    val df_count:DataFrame=df_feature.groupBy(fe.head,fe.tail:_*).count().select("count",fe:_*).withColumn(value,col("count")*0+1)
    val df_count_newName=if(StringUtils.isBlank(name)){
//      fe:+(value + fe.mkString("_","_","_")+"nunique")
      df_count.withColumnRenamed("count",value + fe.mkString("_","_","_")+"nunique")
    }else{
//      fe:+name
      df_count.withColumnRenamed("count",name)
    }
    df.join(df_count_newName,fe,"left").na.fill(0)
  }
}
class Util(spark:SparkSession) {
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

  def loadData(bathPath:String="hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"):(DataFrame, DataFrame) ={
    val sku = "jdata_sku_basic_info.csv"
    val user_basic = "jdata_user_basic_info.csv"
    val user_action = "jdata_user_action.csv"
    val user_order = "jdata_user_order.csv"
    val user_comment = "jdata_user_comment_score.csv"

    val sku_df = getSourceData(basePath + sku)
    //用户信息,user_id,age,sex,user_lv_cd
    val user_df = getSourceData(basePath + user_basic)
    //用户行为，user_id,sku_id,a_date,a_num,a_type
        val action_df=getSourceData(basePath+user_action)
    //订单表，user_id,sku_id,o_id,o_date,o_area,o_sku_num
    val order_df = getSourceData(basePath + user_order)
      .cache()
    //评价表,user_id,comment_create_tm,o_id,score_level
    val comment_df = getSourceData(basePath + user_comment)

    val user_action_df=action_df.withColumn("a_year",getYearFromTime(action_df("a_date")))
      .withColumn("a_month",getMonthFromTime(action_df("a_date")))
      .withColumn("a_day",getDayFromTime(action_df("a_date")))

    val user_order_df=order_df.withColumn("o_year",getYearFromTime(order_df("o_date")))
      .withColumn("o_month",getMonthFromTime(order_df("o_date")))
      .withColumn("o_day",getDayFromTime(order_df("o_date")))

    val user_comment_df=comment_df.withColumnRenamed("comment_create_tm","c_date")
      .withColumn("c_year",getYearFromTime(comment_df("c_date")))
      .withColumn("c_month",getMonthFromTime(comment_df("c_date")))
      .withColumn("c_day",getDayFromTime(comment_df("c_date")))


//    * 把user_order,user_comment,sku,user_info连在一起组成order_comment表
    val order=    user_order_df.join(sku_df,Seq("sku_id"),"left")
        .join(user_df,Seq("user_id"),"left")
      .join(user_comment_df,Seq("user_id","o_id"),"left")

    //把user_action,user_info,sku_info连接一起组成user_action表
    val action=user_action_df.join(sku_df,Seq("sku_id"),"left")
      .join(user_df,Seq("user_id"),"left")
    (order,action)
  }

  val getMonthFromTime=udf{(time:Timestamp)=>time.toLocalDateTime.getMonthValue}
  val getDayFromTime=udf{(time:Timestamp)=>time.toLocalDateTime.getDayOfMonth}
  val getYearFromTime=udf{(time:Timestamp)=>time.toLocalDateTime.getYear}
}

