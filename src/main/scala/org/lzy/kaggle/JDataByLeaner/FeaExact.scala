package org.lzy.kaggle.JDataByLeaner

import java.sql.Timestamp

import org.apache.spark.sql.functions.{count, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Administrator on 2018/5/30.
  */


object FeaExact {
  val basePath = "E:\\dataset\\JData_UserShop\\"
  //  val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
  val startTime = Timestamp.valueOf("2017-03-01 00:00:00")
  val endTime = Timestamp.valueOf("2017-04-30 00:00:00")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val util = new Util(spark)
    val feaExact = new FeaExact(spark)
    //    val (order, action) = util.loadData(basePath)
    //    order.write.parquet(basePath+"cache/order")
    //    action.write.parquet(basePath+"cache/action")
    val order_cache = spark.read.parquet(basePath + "cache/order")
    val action_cache = spark.read.parquet(basePath + "cache/action")
    val user = util.getSourceData(basePath + "jdata_user_basic_info.csv")

    val udf_getLabel = udf { (o_date: Timestamp) => if (o_date.after(startTime)) o_date.toLocalDateTime.getDayOfMonth else 0 }
    //    order_cache.withColumn("new",udf_getLabel(col("o_date"))).show(false)
    val df_label_joined = feaExact.createFeat(startTime, endTime, user, order_cache, action_cache)
    df_label_joined.show(false)
    df_label_joined.write.parquet(basePath+"cache/df_label_joined")
  }


}

class FeaExact(spark: SparkSession) {

  import spark.implicits._

  def createFeat(startTime: Timestamp, endTime: Timestamp, user_df: DataFrame,
                 order: DataFrame, action: DataFrame, test: Boolean = false) = {
    val order_sort_df = order.sort($"o_date")
    val action_sort_df = action.sort($"a_date")
    //预测目标的月份
    val label_month: Int = endTime.toLocalDateTime.getMonthValue

    //定义udf函数，用于求解传入值与指定值的day差
    val udf_dateDiff = udf { date: Timestamp => (endTime.getTime - date.getTime) / (60 * 60 * 24 * 1000) }

    //计算order和action与预测月份之间的时间差值。
    val order_df = order_sort_df.withColumn("day_gap", udf_dateDiff($"o_date"))
    val action_df = action_sort_df.withColumn("day_gap", udf_dateDiff(col("a_date")))

    //构建标签，label1和label2
    val df_label =
      if (test) {
        //如果是测试集，那么label_1=-1,label_2=-1
        val df_label = user_df.withColumn("label_1", col("age") * 0 - 1)
          .withColumn("label_2", col("age") * 0 - 1)
        df_label
      } else {
        //如果是训练集，

        //找到用户在目标月份最早的订单日期，只获取目标时间与需要预测的30和101品类
        val order_label = order_df.filter($"o_month" === label_month).filter(order_df("cate") === 30 || order_df("cate") === 101)
        //只留下每个用户在整个浏览记录中的第一次，最早的一次。sort默认升序
        val label = order_label.sort("o_date").dropDuplicates("user_id")
        //将有浏览数据，并且提取为第一次的和用户数据进行join，获取用户详细信息
        val df_label_1 = user_df.join(label.select("user_id", "o_date"), Seq("user_id"), "left")
        //label_1代表用户在目标月份购买的订单个数，label_2代表用户在一个月的几号下的订单
        //获取每个用户的总下单数，Join到之前的数据中作为标签label1。
        val df_label_2 = Util.featUnique(df_label_1, order_label, Array("user_id"), "o_id", Some("label_1"))
        println("label2")
        df_label_2.show(false)
        df_label_2.printSchema()
        val udf_getLabel = udf { (o_date: Timestamp) =>if(o_date==null) 0 else if (o_date.after(startTime)) o_date.toLocalDateTime.getDayOfMonth else 0 }
        //如果下单时间在开始时间阀值之后，那么获取该时间对应的当月的天数。
        val df_label = df_label_2.withColumn("label_2", udf_getLabel(col("o_date"))).drop("o_date")
        df_label.show(false)
        df_label
      }

    //总体特征
    val order_tmp = order_df.filter($"day_gap" > 0)
    val action_tmp = action_df.filter($"day_gap" > 0)

    val order_tmp_filter = order_tmp.filter($"cate" === 30 || $"cate" === 101)
    val action_tmp_filter = action_tmp.filter($"cate" === 30 || $"cate" === 101)

    val order2Utils = order_tmp_filter.groupBy("user_id").agg(countDistinct("o_id").as("o_id_30_101_nunique"), count("sku_id").as("o_sku_id_30_101_count"),
      sum("o_sku_num").as("o_sku_num_30_101_count"), mean("o_day").as("day_30_101_mean"),
      countDistinct("o_date").as("o_date_30_101_mean"), countDistinct("o_month").as("o_month_30_101_nunique"))
      .select("user_id", "o_id_30_101_nunique", "o_sku_id_30_101_count", "o_sku_num_30_101_count", "day_30_101_mean", "o_date_30_101_mean", "o_month_30_101_nunique")

    val action2Utils = action_tmp_filter.groupBy("user_id").agg(count("sku_id").as("a_sku_id_30_101_count"), countDistinct("a_date").as("a_date_30_101_nunique"))
      .select("user_id", "a_sku_id_30_101_count", "a_date_30_101_nunique")

    val df_label_joined = df_label.join(order2Utils, Seq("user_id"), "left").join(action2Utils, Seq("user_id"), "left").na.fill(0)


    //      .join(order_tmp_filter.groupBy("user_id").agg(countDistinct("o_id").as("o_id_30_101_nunique")).select("o_id_30_101_nunique", "user_id"), Seq("user_id"), "left")
    //      .join(order_tmp_filter.groupBy("user_id").agg(count("sku_id").as("o_sku_id_30_101_count")).select("o_sku_id_30_101_count", "user_id"), Seq("user_id"), "left")
    //      .join(order_tmp_filter.groupBy("user_id").agg(sum("o_sku_num").as("o_sku_num_30_101_count")).select("o_sku_num_30_101_count", "user_id"), Seq("user_id"), "left")
    //      .join(order_tmp_filter.groupBy("user_id").agg(mean("o_day").as("day_30_101_mean")).select("day_30_101_mean", "user_id"), Seq("user_id"), "left")
    //      .join(order_tmp_filter.groupBy("user_id").agg(countDistinct("o_date").as("o_date_30_101_mean")).select("o_date_30_101_mean", "user_id"), Seq("user_id"), "left")
    //      .join(order_tmp_filter.groupBy("user_id").agg(countDistinct("o_month").as("o_month_30_101_nunique")).select("o_month_30_101_nunique", "user_id"), Seq("user_id"), "left")
    //      .join(action_tmp_filter.groupBy("user_id").agg(count("sku_id").as("a_sku_id_30_101_count")).select("a_sku_id_30_101_count", "user_id"), Seq("user_id"), "left")
    //      .join(action_tmp_filter.groupBy("user_id").agg(countDistinct("a_date").as("a_date_30_101_nunique")).select("a_date_30_101_nunique", "user_id"), Seq("user_id"), "left")
    //      .na.fill(0)

    df_label_joined
  }


}