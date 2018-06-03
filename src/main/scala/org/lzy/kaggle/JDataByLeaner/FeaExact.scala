package org.lzy.kaggle.JDataByLeaner

import java.sql.Timestamp

import org.apache.spark.sql.functions.{count, _}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by Administrator on 2018/5/30.
  * spark-submit --master yarn-cluster --queue xkl --driver-memory 10g  --num-executors 9 --executor-cores 4 --executor-memory 8g --class org.lzy.kaggle.JDataByLeaner.FeaExact SparkML.jar
  */


object FeaExact {
  //  val basePath = "E:\\dataset\\JData_UserShop\\"
  val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
      //      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val util = new Util(spark)
    val feaExact = new FeaExact(spark, basePath)
//    val (order, action) = util.loadData(basePath)
//    order.write.mode(SaveMode.Overwrite).parquet(basePath + "cache/order")
//    action.write.mode(SaveMode.Overwrite).parquet(basePath + "cache/action")
    val order_cache = spark.read.parquet(basePath + "cache/order").cache()
    val action_cache = spark.read.parquet(basePath + "cache/action").cache()
    val user = util.getSourceData(basePath + "jdata_user_basic_info.csv").cache()

    feaExact.gen_Vali(order_cache, action_cache, user)
    feaExact.genTest(order_cache, action_cache, user)
    println("完毕：")

  }


}

class FeaExact(spark: SparkSession, basePath: String) {

  import spark.implicits._


  def gen_Vali(order: DataFrame, action: DataFrame, user: DataFrame) = {
    val train = createFeat(getTime("2017-02-01"), getTime("2017-03-01"), order, action, user)
      .union(createFeat(getTime("2017-01-01"), getTime("2017-02-01"), order, action, user))
      .union(createFeat(getTime("2016-12-01"), getTime("2017-01-01"), order, action, user))
      .union(createFeat(getTime("2016-11-01"), getTime("2016-12-01"), order, action, user))
      .union(createFeat(getTime("2016-10-01"), getTime("2016-11-01"), order, action, user))

    val test = createFeat(getTime("2017-03-01"), getTime("2017-04-01"), order, action, user)
    train.write.mode(SaveMode.Overwrite).parquet(basePath + "cache/vali_train")
    test.write.mode(SaveMode.Overwrite).parquet(basePath + "cache/vali_test")
  }

  def genTest(order: DataFrame, action: DataFrame, user: DataFrame) = {
    val train = createFeat(getTime("2017-03-01"), getTime("2017-04-01"), order, action, user)
      .union(createFeat(getTime("2017-02-01"), getTime("2017-03-01"), order, action, user))
      .union(createFeat(getTime("2017-01-01"), getTime("2017-02-01"), order, action, user))
      .union(createFeat(getTime("2016-12-01"), getTime("2017-01-01"), order, action, user))
      .union(createFeat(getTime("2016-11-01"), getTime("2016-12-01"), order, action, user))
      .union(createFeat(getTime("2016-10-01"), getTime("2016-11-01"), order, action, user))
    val test = createFeat(getTime("2017-04-01"), getTime("2017-05-01"), order, action, user)

    train.repartition(100).write.mode(SaveMode.Overwrite).parquet(basePath + "cache/test_train")
    test.repartition(100).write.mode(SaveMode.Overwrite).parquet(basePath + "cache/test_test")
  }

  def getTime(yyyy_MM_dd: String) = {
    Timestamp.valueOf(yyyy_MM_dd + " 00:00:00")
  }

  def createFeat(startTime: Timestamp, endTime: Timestamp, order: DataFrame, action: DataFrame, user_df: DataFrame, test: Boolean = false) = {
    println("开始时间："+startTime)
    println("结束时间："+endTime)
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
        val udf_getLabel = udf { (o_date: Timestamp) => if (o_date == null) 0 else if (o_date.after(startTime)) o_date.toLocalDateTime.getDayOfMonth else 0 }
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


    //      .jDoin(order_tmp_filter.groupBy("user_id").agg(countDistinct("o_id").as("o_id_30_101_nunique")).select("o_id_30_101_nunique", "user_id"), Seq("user_id"), "left")
    //      .join(order_tmp_filter.groupBy("user_id").agg(count("sku_id").as("o_sku_id_30_101_count")).select("o_sku_id_30_101_count", "user_id"), Seq("user_id"), "left")
    //      .join(order_tmp_filter.groupBy("user_id").agg(sum("o_sku_num").as("o_sku_num_30_101_count")).select("o_sku_num_30_101_count", "user_id"), Seq("user_id"), "left")
    //      .join(order_tmp_filter.groupBy("user_id").agg(mean("o_day").as("day_30_101_mean")).select("day_30_101_mean", "user_id"), Seq("user_id"), "left")
    //      .join(order_tmp_filter.groupBy("user_id").agg(countDistinct("o_date").as("o_date_30_101_mean")).select("o_date_30_101_mean", "user_id"), Seq("user_id"), "left")
    //      .join(order_tmp_filter.groupBy("user_id").agg(countDistinct("o_month").as("o_month_30_101_nunique")).select("o_month_30_101_nunique", "user_id"), Seq("user_id"), "left")
    //      .join(action_tmp_filter.groupBy("user_id").agg(count("sku_id").as("a_sku_id_30_101_count")).select("a_sku_id_30_101_count", "user_id"), Seq("user_id"), "left")
    //      .join(action_tmp_filter.groupBy("user_id").agg(countDistinct("a_date").as("a_date_30_101_nunique")).select("a_date_30_101_nunique", "user_id"), Seq("user_id"), "left")
    //      .na.fill(0)
    val df_label_7_df = getFeatureBySubDay(7, order_df, action_df, df_label_joined)
    val df_label_7And14_df = getFeatureBySubDay(14, order_df, action_df, df_label_7_df)
    val df_label_7And14And30_df = getFeatureBySubDay(30, order_df, action_df, df_label_7And14_df)
    val df_label_7And14And30And90_df = getFeatureBySubDay(90, order_df, action_df, df_label_7And14And30_df)
    val df_label_7And14And30And90And190_df = getFeatureBySubDay(180, order_df, action_df, df_label_7And14And30And90_df)

    df_label_7And14And30And90And190_df.na.fill(0)
  }


  def getFeatureBySubDay(i: Int, order_df: DataFrame, action_df: DataFrame, df_label_joined: DataFrame) = {
    println("天数为："+i)
    val order_tmp_byDay = order_df.filter($"day_gap" < i && $"day_gap" > 0)
    val action_tmp_byDay = action_df.filter($"day_gap" < i && $"day_gap" > 0)
    val a = s"AD${i}_"
    val o = s"OD${i}_"
    //#######################################################################################################################################
    //获取（30，101）
    val order_tmp_30And101 = order_tmp_byDay.filter($"cate" === 30 || $"cate" === 101)
      .groupBy("user_id").agg(countDistinct("o_id").as(o + "o_id_30_101_nunique"),
      count("sku_id").as(o + "sku_id_30_101_count"),
      sum("o_sku_num").as(o + "sku_num_30_101_count"),
      max("o_day").as(o + "day_30_101_max"),
      mean("o_day").as(o + "day_30_101_mean"),
      countDistinct("o_date").as(o + "o_date_30_101_nunique"),
      countDistinct("o_month").as(o + "o_month_30_101_nunique"))
      .select("user_id", o + "o_id_30_101_nunique", o + "sku_id_30_101_count", o + "sku_num_30_101_count", o + "day_30_101_max", o + "day_30_101_mean", o + "o_date_30_101_nunique", o + "o_month_30_101_nunique")
    //#######################################################################################################################################
    //获取（30）
    val order_tmp_30 = order_tmp_byDay.filter($"cate" === 30)
      .groupBy("user_id").agg(countDistinct("o_id").as(o + "o_id_30_nunique"),
      count("sku_id").as(o + "sku_id_30_count"),
      sum("o_sku_num").as(o + "sku_num_30_count"),
      max("o_day").as(o + "day_30_max"),
      mean("o_day").as(o + "day_30_mean"),
      countDistinct("o_date").as(o + "o_date_30_nunique"),
      countDistinct("o_month").as(o + "o_month_30_nunique"))
      .select("user_id", o + "o_id_30_nunique", o + "sku_id_30_count", o + "sku_num_30_count", o + "day_30_max", o + "day_30_mean", o + "o_date_30_nunique", o + "o_month_30_nunique")
    //#######################################################################################################################################
    //获取（101）
    val order_tmp_101 = order_tmp_byDay.filter($"cate" === 101)
      .groupBy("user_id").agg(countDistinct("o_id").as(o + "o_id_101_nunique"),
      count("sku_id").as(o + "sku_id_101_count"),
      sum("o_sku_num").as(o + "sku_num_101_count"),
      max("o_day").as(o + "day_101_max"),
      mean("o_day").as(o + "day_101_mean"),
      countDistinct("o_date").as(o + "o_date_101_nunique"),
      countDistinct("o_month").as(o + "o_month_101_nunique"))
      .select("user_id", o + "o_id_101_nunique", o + "sku_id_101_count", o + "sku_num_101_count", o + "day_101_max", o + "day_101_mean", o + "o_date_101_nunique", o + "o_month_101_nunique")
    //#######################################################################################################################################
    //获取!（30,101）
    val order_tmp_other = order_tmp_byDay.filter($"cate" =!= 30 && $"cate" =!= 101)
      .groupBy("user_id").agg(countDistinct("o_id").as(o + "o_id_other_nunique"),
      count("sku_id").as(o + "sku_id_other_count"),
      sum("o_sku_num").as(o + "sku_num_other_count"))
      .select("user_id", o + "o_id_other_nunique", o + "sku_id_other_count", o + "sku_num_other_count")
    //#######################################################################################################################################
    //用户当月首次订单是哪一天
    val order_tmp_firstDay = order_tmp_byDay.filter($"cate" === 30 || $"cate" === 101).dropDuplicates("user_id").select("user_id", "o_day").withColumnRenamed( "o_day",o + "o_date_30_101_firstday")
    /*
    * action相关
    * */
    //用户在当月的两种行为
    val action_tmp_type_1 = action_tmp_byDay.filter($"a_type" === 1)
    val action_tmp_type_2 = action_tmp_byDay.filter($"a_type" === 2)

    //#######################################################################################################################################
    //获取挡雨（30.101），30，101，（！30，101）的订单
    //获取（30，101）
    val action_tmp_30And101 = action_tmp_byDay.filter($"cate" === 30 || $"cate" === 101)
      .groupBy("user_id").agg(
      count("sku_id").as(a + "sku_id_30_101_count"),
      countDistinct("a_date").as(a + "a_date_30_101_nunique"),
      countDistinct("sku_id").as(a + "sku_id_30_101_nunique"))
      .select("user_id", a + "sku_id_30_101_count", a + "a_date_30_101_nunique", a + "sku_id_30_101_nunique")
    val action_tmp_30And101_type1 = action_tmp_type_1.filter($"cate" === 30 || $"cate" === 101)
      .groupBy("user_id").agg(count("sku_id").as(a + "sku_id_type1_30_101_count")).select("user_id", a + "sku_id_type1_30_101_count")
    val action_tmp_30And101_type2 = action_tmp_type_2.filter($"cate" === 30 || $"cate" === 101)
      .groupBy("user_id").agg(count("sku_id").as(a + "sku_id_type2_30_101_count")).select("user_id", a + "sku_id_type2_30_101_count")
    //#######################################################################################################################################
    //获取（30）
    val action_tmp_30 = action_tmp_byDay.filter($"cate" === 30)
      .groupBy("user_id").agg(
      count("sku_id").as(a + "sku_id_30_count"),
      countDistinct("a_date").as(a + "a_date_30_nunique"),
      countDistinct("sku_id").as(a + "sku_id_30_nunique"))
      .select("user_id", a + "sku_id_30_count", a + "a_date_30_nunique", a + "sku_id_30_nunique")
    val action_tmp_30_type1 = action_tmp_type_1.filter($"cate" === 30)
      .groupBy("user_id").agg(count("sku_id").as(a + "sku_id_type1_30_count")).select("user_id", a + "sku_id_type1_30_count")
    val action_tmp_30_type2 = action_tmp_type_2.filter($"cate" === 30)
      .groupBy("user_id").agg(count("sku_id").as(a + "sku_id_type2_30_count")).select("user_id", a + "sku_id_type2_30_count")
    //#######################################################################################################################################
    //获取（101）
    val action_tmp_101 = action_tmp_byDay.filter($"cate" === 101)
      .groupBy("user_id").agg(
      count("sku_id").as(a + "sku_id_101_count"),
      countDistinct("a_date").as(a + "a_date_101_nunique"),
      countDistinct("sku_id").as(a + "sku_id_101_nunique"))
      .select("user_id", a + "sku_id_101_count", a + "a_date_101_nunique", a + "sku_id_101_nunique")
    val action_tmp_101_type1 = action_tmp_type_1.filter($"cate" === 101)
      .groupBy("user_id").agg(count("sku_id").as(a + "sku_id_type1_101_count")).select("user_id", a + "sku_id_type1_101_count")
    val action_tmp_101_type2 = action_tmp_type_2.filter($"cate" === 101)
      .groupBy("user_id").agg(count("sku_id").as(a + "sku_id_type2_101_count")).select("user_id", a + "sku_id_type2_101_count")

    df_label_joined.join(order_tmp_30And101, Seq("user_id"), "left")
      .join(order_tmp_30, Seq("user_id"), "left")
      .join(order_tmp_101, Seq("user_id"), "left")
      .join(order_tmp_other, Seq("user_id"), "left")
      .join(order_tmp_firstDay, Seq("user_id"), "left")
      .join(action_tmp_30And101, Seq("user_id"), "left")
      .join(action_tmp_30And101_type1, Seq("user_id"), "left")
      .join(action_tmp_30And101_type2, Seq("user_id"), "left")
      .join(action_tmp_30, Seq("user_id"), "left")
      .join(action_tmp_30_type1, Seq("user_id"), "left")
      .join(action_tmp_30_type2, Seq("user_id"), "left")
      .join(action_tmp_101, Seq("user_id"), "left")
      .join(action_tmp_101_type1, Seq("user_id"), "left")
      .join(action_tmp_101_type2, Seq("user_id"), "left")
  }
}