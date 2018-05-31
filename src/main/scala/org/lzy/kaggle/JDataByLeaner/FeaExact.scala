package org.lzy.kaggle.JDataByLeaner

import java.sql.Timestamp

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Administrator on 2018/5/30.
  */


object FeaExact {
  val basePath = "E:\\dataset\\JData_UserShop\\"

  //  val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
      .master("local[*]")
      .getOrCreate()
    val util = new Util(spark)
    val (order, action) = util.loadData(basePath)
  }


}

class FeaExact(spark: SparkSession) {

  import spark.implicits._

  def createFeat(startTime: Timestamp, endTime: Timestamp, user_df: DataFrame,
                 order: DataFrame, action: DataFrame, test: Boolean = false): Unit = {
    val order_df = order.sort($"o_date")
    val action_df = action.sort($"a_date")
    //预测目标月份的数据
    val label_month: Int = endTime.toLocalDateTime.getMonthValue

    //计算order和action与预测月份之间的时间差值。
    order_df.withColumn("day_gap", order_df.select($"o_date".as[Timestamp]).map(date => (endTime.getTime - date.getTime) / (60 * 60 * 24 * 1000)).col("o_date"))
    action_df.withColumn("day_gap", action_df.select($"a_date".as[Timestamp]).map(date => (endTime.getTime - date.getTime) / (60 * 60 * 24 * 1000)).col("a_date"))
    action_df.select($"a_date".as[Timestamp]).map(date => (endTime.getTime - date.getTime) / (60 * 60 * 24 * 1000))
    val df_label =
      if (test) {
        val df_label = user_df.withColumn("label_1", col("age") * 0 - 1)
          .withColumn("label_2", col("age") * 0 - 1)
        df_label
      } else {
        //找到用户在目标月份最早的订单日期
        val order_label = order_df.filter(order_df("o_date") === label_month)
          .filter(order_df("cate") === 30 || order_df("cate") === 101)
        val label = order_label.sort("o_date").dropDuplicates("user_id")
        val df_label_1 = user_df.join(label.select("user_id", "o_date"), Seq("user_id"), "left")

        //label_1代表用户在目标月份购买的订单个数，label_2代表用户在一个月的几号下的订单
        val df_label_2 = Util.featUnique(df_label_1, order_label, Array("user_id"), "o_id", Some("label_1"))
        val df_label = df_label_2.withColumn("label_2", df_label_2.select($"o_date".as[Timestamp], $"o_day".as[Int]).map(date2day => {
          if (date2day._1.after(startTime)) {
            date2day._2
          } else {
            0
          }
        }).toDF("label_2").col("label_2")).drop("o_date")
        df_label
      }

    //总体特征
    val order_tmp=order.filter($"day_gap" > 0)
    val action_tmp=action.filter($"day_gap" > 0)



  }


}