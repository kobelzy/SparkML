package org.lzy.kaggle.JDataByLeaner

import java.sql.Timestamp

import org.apache.spark.sql.functions.{count, _}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by Administrator on 2018/5/30.
  * spark-submit --master yarn-cluster --queue lzy --driver-memory 10g --conf spark.driver.maxResultSize=5g  --num-executors 6 --executor-cores 6 --executor-memory 10g --class org.lzy.kaggle.JDataByLeaner.FeaExact SparkML.jar
  * spark-submit --master yarn-cluster --queue lzy --driver-memory 6g --conf spark.driver.maxResultSize=4g     --num-executors 8 --executor-cores 4 --executor-memory 6g --jars    /root/lzy/xgboost/jvm-packages/xgboost4j-spark/target/xgboost4j-spark-0.8-SNAPSHOT-jar-with-dependencies.jar    --class org.lzy.kaggle.JDataByLeaner.FeaExact SparkML.jar
  */


object FeaExact {
  //  val basePath = "E:\\dataset\\JData_UserShop\\"
  val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
      //      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val conf = spark.conf
    val sc = spark.sparkContext
    val config = sc.getConf
    //    config.set("spark.driver.maxResultSize","0")
    config.set("spark.debug.maxToStringFields", "100")
    config.set("spark.shuffle.io.maxRetries", "60")
    config.set("spark.default.parallelism", "54")
    config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val util = new Util(spark)
    val feaExact = new FeaExact(spark, basePath)
    //    val (order, action) = util.loadData(basePath)
    //    order.write.mode(SaveMode.Overwrite).parquet(basePath + "cache/order")
    //    action.write.mode(SaveMode.Overwrite).parquet(basePath + "cache/action")
    val order_cache = spark.read.parquet(basePath + "cache/order").repartition(60).cache()
    val action_cache = spark.read.parquet(basePath + "cache/action").repartition(60).cache()
    val user = util.getSourceData(basePath + "jdata_user_basic_info.csv").repartition(60).cache()

    //    feaExact.getAndSaveValiData(order_cache, action_cache, user)
    //    feaExact.getAndSaveTestData(order_cache, action_cache, user)
    feaExact.save11To4MonthData(order_cache, action_cache, user)

    println("完毕：")

  }


}

class FeaExact(spark: SparkSession, basePath: String) {

  import spark.implicits._

  /** *
    * 功能实现:
    * 获取从9月份开始到4月份的 相关用户特征，包含每个月中参与订单的用户，以及其过去一段时间的消费特征。
    * Author: Lzy
    * Date: 2018/6/11 9:50
    * Param: [order, action, user]
    * Return: void
    */
  def save11To4MonthData(order: DataFrame, action: DataFrame, user: DataFrame
                         //                       ,months:Array[Int]
                        ) = {

    createFeat(getTime("2016-09-01"), getTime("2016-10-01"), order, action, user).write.mode(SaveMode.Overwrite).parquet(basePath + "cache/trainMonth/09")
    createFeat(getTime("2016-10-01"), getTime("2016-11-01"), order, action, user).write.mode(SaveMode.Overwrite).parquet(basePath + "cache/trainMonth/10")
    createFeat(getTime("2016-11-01"), getTime("2016-12-01"), order, action, user).write.mode(SaveMode.Overwrite).parquet(basePath + "cache/trainMonth/11")
    createFeat(getTime("2016-12-01"), getTime("2017-01-01"), order, action, user).write.mode(SaveMode.Overwrite).parquet(basePath + "cache/trainMonth/12")
    createFeat(getTime("2017-01-01"), getTime("2017-02-01"), order, action, user).write.mode(SaveMode.Overwrite).parquet(basePath + "cache/trainMonth/01")
    createFeat(getTime("2017-02-01"), getTime("2017-03-01"), order, action, user).write.mode(SaveMode.Overwrite).parquet(basePath + "cache/trainMonth/02")
    createFeat(getTime("2017-03-01"), getTime("2017-04-01"), order, action, user).write.mode(SaveMode.Overwrite).parquet(basePath + "cache/trainMonth/03")
    createFeat(getTime("2017-04-01"), getTime("2017-05-01"), order, action, user).write.mode(SaveMode.Overwrite).parquet(basePath + "cache/trainMonth/04")
    //  for(month<-months){
    //    createFeat(getTime(s"2016-${month}-01"), getTime(s"2016-${month+1}-01"), order, action, user).write.mode(SaveMode.Overwrite).parquet(basePath + s"cache/trainMonth/$month")
    //  }

  }

  def getAndSaveValiData(order: DataFrame, action: DataFrame, user: DataFrame) = {
    val test = createFeat(getTime("2017-03-01"), getTime("2017-04-01"), order, action, user)
    test.write.mode(SaveMode.Overwrite).parquet(basePath + "cache/vali_test_start12")
    val train = createFeat(getTime("2017-02-01"), getTime("2017-03-01"), order, action, user)
      .union(createFeat(getTime("2017-01-01"), getTime("2017-02-01"), order, action, user))
      .union(createFeat(getTime("2016-12-01"), getTime("2017-01-01"), order, action, user))
    //      .union(createFeat(getTime("2016-11-01"), getTime("2016-12-01"), order, action, user))
    //      .union(createFeat(getTime("2016-10-01"), getTime("2016-11-01"), order, action, user))
    train.write.mode(SaveMode.Overwrite).parquet(basePath + "cache/vali_train_start12")

    createFeat(getTime("2016-10-01"), getTime("2016-11-01"), order, action, user).write.mode(SaveMode.Overwrite).parquet(basePath + "cache/vali/train/10")


    createFeat(getTime("2017-03-01"), getTime("2017-04-01"), order, action, user).write.mode(SaveMode.Overwrite).parquet(basePath + "cache/vali/test/10")
  }

  def getAndSaveTestData(order: DataFrame, action: DataFrame, user: DataFrame) = {
    val test = createFeat(getTime("2017-04-01"), getTime("2017-05-01"), order, action, user)

    test.write.mode(SaveMode.Overwrite).parquet(basePath + "cache/test_test_start12")

    val train = createFeat(getTime("2017-03-01"), getTime("2017-04-01"), order, action, user)
      .union(createFeat(getTime("2017-02-01"), getTime("2017-03-01"), order, action, user))
      .union(createFeat(getTime("2017-01-01"), getTime("2017-02-01"), order, action, user))
      .union(createFeat(getTime("2016-12-01"), getTime("2017-01-01"), order, action, user))
    //      .union(createFeat(getTime("2016-11-01"), getTime("2016-12-01"), order, action, user))
    //      .union(createFeat(getTime("2016-10-01"), getTime("2016-11-01"), order, action, user))

    train.write.mode(SaveMode.Overwrite).parquet(basePath + "cache/test_train_start12")
  }

  def getTime(yyyy_MM_dd: String) = {
    Timestamp.valueOf(yyyy_MM_dd + " 00:00:00")
  }

  def createFeat(startTime: Timestamp, endTime: Timestamp, order: DataFrame, action: DataFrame, user_df: DataFrame, test: Boolean = false) = {
    println("开始时间：" + startTime) //04-01
    println("结束时间：" + endTime) //05-01
    //预测目标的月份
    val label_month: Int = endTime.toLocalDateTime.getMonthValue //5月

    //定义udf函数，用于求解传入值与指定值的day差
    //    val udf_dateDiff = udf { date: Timestamp => (endTime.getTime - date.getTime) / (60 * 60 * 24 * 1000) }

    //计算order和action与预测月份之间的时间差值。
    val order_df = order.sort($"o_date").withColumn("day_gap", datediff($"o_date", lit(endTime))) //距离5月1号的天数，一定为负数,其他月份不一定为负数。
      .withColumn("o_sku_num",when($"o_sku_num" < 20,$"o_sku_num").otherwise(20))
    val action_df = action.sort($"a_date").withColumn("day_gap", datediff($"a_date", lit(endTime)))
      .withColumn("a_num",when($"a_num" < 50,$"a_num").otherwise(50))

    //构建标签，label1和label2
    //label_1代表用户在目标月份购买的订单个数，label_2代表用户在一个月的几号下的订单
    val order_label_df =
    if (test) {
      //如果是测试集，那么label_1=-1,label_2=-1
      val df_label = user_df.withColumn("label_1", lit(-1)).withColumn("label_2", lit(-1))
      df_label
    } else {
      //如果是训练集

      //找到用户在目标月份最早的订单日期，只获取目标时间与需要预测的30和101品类
      val order_targetMonth_df = order_df.filter($"o_month" === label_month).filter(order_df("cate") === 30 || order_df("cate") === 101)
      //只留下每个用户在整个浏览记录中的第一次，最早的一次。sort默认升序
      val order_firstDate_df = order_targetMonth_df.sort("o_date").dropDuplicates("user_id").select("user_id", "o_date")
      //将有浏览数据，并且提取为第一次的和用户数据进行join，获取用户详细信息
      //user_id,o_date,age,等
      val order_user2firstDate_df = user_df.join(order_firstDate_df, Seq("user_id"), "left")
      //获取每个用户的总下单数，Join到之前的数据中作为标签label1。
      //lable_1代表用户在目标月份购买的订单个数，label_2代表用户在一个月的几号下的订单
      val order_label1_df = Util.featUnique(order_user2firstDate_df, order_targetMonth_df, Seq("user_id"), "o_id", Some("label_unique1"))
        .withColumn("label_1", when($"label_unique1" <= 3, $"label_unique1").otherwise(3))
        .drop("label_unique1")
      println("label2")
      val udf_getLabel2 = udf { (o_date: Timestamp) => if (o_date == null) 0 else if (o_date.after(startTime) || o_date.equals(startTime)) o_date.toLocalDateTime.getDayOfMonth - 1 else 0 }
      //如果下单时间在开始时间之后，那么获取该时间对应的当月的几号。
      val order_label1Andlabel2_df = order_label1_df
        //                .withColumn("label_2", udf_getLabel2($"o_date"))
        .withColumn("label_2", when($"o_date" >= startTime, dayofmonth($"o_date")).otherwise(0))
        .drop("o_date")
      order_label1Andlabel2_df.show(false)
      order_label1Andlabel2_df
    }

    /*
    总体特征
    */
    //选择在endTime之前的所有值
    val order_30And101_BeforeEnd_df = order_df.filter($"day_gap" < 0).filter($"cate" === 30 || $"cate" === 101).cache()
    val action_30And101_BeforeEnd_df = action_df.filter($"day_gap" < 0).filter($"cate" === 30 || $"cate" === 101).cache()


    val order2Utils_df = order_30And101_BeforeEnd_df.groupBy("user_id")
      .agg(countDistinct("o_id").as("o_id_30_101_nunique"),
        countDistinct("sku_id").as("o_sku_id_30_101_nunique"),
        count("sku_id").as("o_sku_id_30_101_count"),
        sum("o_sku_num").as("o_sku_num_30_101_count"),
        mean("o_day").as("day_30_101_mean"),
        countDistinct("o_date").as("o_date_30_101_mean"),
        countDistinct("o_month").as("o_month_30_101_nunique"),
        countDistinct("sku_id").as("sku_id_30_101_nunique"),
        sum("price").as("price_sum"),
//        min("price").as("price_min"),
//        max("price").as("price_max"),
        mean("price").as("price_mean")

      )
      .select("user_id", "o_id_30_101_nunique", "o_sku_id_30_101_nunique","o_sku_id_30_101_count", "o_sku_num_30_101_count", "day_30_101_mean", "o_date_30_101_mean", "o_month_30_101_nunique",
        "sku_id_30_101_nunique", "price_sum",
//        "price_min","price_max",
    "price_mean"
      )

    val action2Utils_df = action_30And101_BeforeEnd_df.groupBy("user_id")
      .agg(count("sku_id").as("a_sku_id_30_101_count"),
        countDistinct("a_date").as("a_date_30_101_nunique"))
      .select("user_id", "a_sku_id_30_101_count", "a_date_30_101_nunique")

    /*
    评价
     */
//    val comment1_df = order_30And101_BeforeEnd_df.filter($"score_level" === 1).groupBy("user_id").agg(sum($"score_level").as("score_1_sum")).select("user_id", "score_1_sum")
//    val comment2_df = order_30And101_BeforeEnd_df.filter($"score_level" === 2).groupBy("user_id").agg(sum($"score_level").as("score_2_sum")).select("user_id", "score_2_sum")
//    val comment3_df = order_30And101_BeforeEnd_df.filter($"score_level" === 3).groupBy("user_id").agg(sum($"score_level").as("score_3_sum")).select("user_id", "score_3_sum")
//    val comment_df = order_30And101_BeforeEnd_df.filter($"score_level" > 0).groupBy("user_id").agg(mean($"score_level").as("score_mean"), stddev($"score_level").as("score_std")).select("user_id", "score_mean", "score_std")
//
//    val commentAnd1_df = comment_df.join(comment1_df, "user_id")
//    val comment2And3_df = comment2_df.join(comment3_df, "user_id")
//    val comment = commentAnd1_df.join(comment2And3_df, "user_id")
    //用当月有订单的用户来联合其在当月的一些基本特征。
    val df_label_joined = order_label_df.join(broadcast(order2Utils_df), Seq("user_id"), "left").join(broadcast(action2Utils_df), Seq("user_id"), "left")
//      .join(broadcast(comment), Seq("user_id"), "left")
            .na.fill(0)
    //获取用户在endTime7天前，14天前，30天前，90天前，180天前的相关统计特征，并联合。
    val df_label_7_df = getFeatureBySubDay(7, order_df, action_df, df_label_joined)
    val df_label_7And14_df = getFeatureBySubDay(14, order_df, action_df, df_label_7_df)
    val df_label_7And14And30_df = getFeatureBySubDay(21, order_df, action_df, df_label_7And14_df)
    val df_label_7And14And30And90_df = getFeatureBySubDay(30, order_df, action_df, df_label_7And14And30_df)
    val df_label_7And14And30And90And180_df = getFeatureBySubDay(60, order_df, action_df, df_label_7And14And30And90_df)
    val df_label_7And14And30And90And180And90_df = getFeatureBySubDay(90, order_df, action_df, df_label_7And14And30And90And180_df)
    val df_label_7And14And30And90And180And90And180_df = getFeatureBySubDay(180, order_df, action_df, df_label_7And14And30And90And180And90_df)
    order_30And101_BeforeEnd_df.unpersist()
    action_30And101_BeforeEnd_df.unpersist()

    df_label_7And14And30And90And180And90And180_df
  }


  def getFeatureBySubDay(i: Int, order_df: DataFrame, action_df: DataFrame, df_label_joined: DataFrame) = {
    println("天数为：" + i)
    val order_tmp_byDay = order_df.filter($"day_gap" >= -i && $"day_gap" < 0)
    val action_tmp_byDay = action_df.filter($"day_gap" >= -i && $"day_gap" < 0)
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
      countDistinct("o_month").as(o + "o_month_30_101_nunique"),
      //添加测试
      var_samp("o_day").as(o + "o_id_30_101_var"),
      stddev_samp("o_day").as(o + "o_id_30_101_std"),
//      skewness("o_day").as(o + "o_id_30_101_skewness"),
//      var_samp("o_sku_num").as(o + "o_sku_num_30_101_var"),
      stddev_samp("o_sku_num").as(o + "o_sku_num_30_101_std"),
      skewness("o_sku_num").as(o + "o_sku_num_30_101_skewness")
    )
      .select("user_id", o + "o_id_30_101_nunique", o + "sku_id_30_101_count", o + "sku_num_30_101_count", o + "day_30_101_max", o + "day_30_101_mean", o + "o_date_30_101_nunique", o + "o_month_30_101_nunique",
        o + "o_id_30_101_var", o + "o_id_30_101_std", o + "o_sku_num_30_101_std", o + "o_sku_num_30_101_skewness"
      )
    //#######################################################################################################################################
    //获取（30）
    val order_tmp_30 = order_tmp_byDay.filter($"cate" === 30)
      .groupBy("user_id").agg(countDistinct("o_id").as(o + "o_id_30_nunique"),
      count("sku_id").as(o + "sku_id_30_count"),
      sum("o_sku_num").as(o + "sku_num_30_count"),
      max("o_day").as(o + "day_30_max"),
      mean("o_day").as(o + "day_30_mean"),
      countDistinct("o_date").as(o + "o_date_30_nunique"),
      countDistinct("o_month").as(o + "o_month_30_nunique"),
      //添加测试
      var_samp("o_day").as(o + "o_id_30_var"),
      stddev_samp("o_day").as(o + "o_id_30_std"),
//      skewness("o_day").as(o + "o_id_30_skewness"),
//      var_samp("o_sku_num").as(o + "o_sku_num_30_var"),
      stddev_samp("o_sku_num").as(o + "o_sku_num_30_std"),
      skewness("o_sku_num").as(o + "o_sku_num_30_skewness")
    )
      .select("user_id", o + "o_id_30_nunique", o + "sku_id_30_count", o + "sku_num_30_count", o + "day_30_max", o + "day_30_mean", o + "o_date_30_nunique", o + "o_month_30_nunique",
        o + "o_id_30_var", o + "o_id_30_std", o + "o_sku_num_30_std", o + "o_sku_num_30_skewness"
      )
    //#######################################################################################################################################
    //获取（101）
    val order_tmp_101 = order_tmp_byDay.filter($"cate" === 101)
      .groupBy("user_id").agg(countDistinct("o_id").as(o + "o_id_101_nunique"),
      count("sku_id").as(o + "sku_id_101_count"),
      sum("o_sku_num").as(o + "sku_num_101_count"),
//      max("o_day").as(o + "day_101_max"),
//      mean("o_day").as(o + "day_101_mean"),
      countDistinct("o_date").as(o + "o_date_101_nunique"),
      countDistinct("o_month").as(o + "o_month_101_nunique"),
      //添加测试
      var_samp("o_day").as(o + "o_id_101_var"),
      stddev_samp("o_day").as(o + "o_id_101_std"),
//      skewness("o_day").as(o + "o_id_101_skewness"),
//      var_samp("o_sku_num").as(o + "o_sku_num_101_var"),
      stddev_samp("o_sku_num").as(o + "o_sku_num_101_std"),
      skewness("o_sku_num").as(o + "o_sku_num_101_skewness")
    )
      .select("user_id", o + "o_id_101_nunique", o + "sku_id_101_count", o + "sku_num_101_count",
//        o + "day_101_max", o + "day_101_mean",
        o + "o_date_101_nunique", o + "o_month_101_nunique",
        o + "o_id_101_var", o + "o_id_101_std",  o + "o_sku_num_101_std", o + "o_sku_num_101_skewness"
      )
    //#######################################################################################################################################
    //获取!（30,101）
    val order_tmp_other = order_tmp_byDay.filter($"cate" =!= 30 && $"cate" =!= 101)
      .groupBy("user_id").agg(countDistinct("o_id").as(o + "o_id_other_nunique"),
      count("sku_id").as(o + "sku_id_other_count"),
      sum("o_sku_num").as(o + "sku_num_other_count"))
      .select("user_id", o + "o_id_other_nunique", o + "sku_id_other_count", o + "sku_num_other_count")
    //#######################################################################################################################################
    //用户当月首次订单是哪一天
    val order_tmp_firstDay = order_tmp_byDay.filter($"cate" === 30 || $"cate" === 101).dropDuplicates("user_id").select("user_id", "o_day").withColumnRenamed("o_day", o + "o_date_30_101_firstday")
    /*
    * action相关
    * */
    //用户在当月的两种行为
    val action_tmp_type_1 = action_tmp_byDay.filter($"a_type" === 1)
    val action_tmp_type_2 = action_tmp_byDay.filter($"a_type" === 2)

    //#######################################################################################################################################
    //获取当月（30.101），30，101，（！30，101）的订单
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
      countDistinct("a_date").as(a + "a_date_101_nunique")
//      ,countDistinct("sku_id").as(a + "sku_id_101_nunique")
    )
      .select("user_id", a + "sku_id_101_count", a + "a_date_101_nunique"
//        , a + "sku_id_101_nunique"
      )
    val action_tmp_101_type1 = action_tmp_type_1.filter($"cate" === 101)
      .groupBy("user_id").agg(count("sku_id").as(a + "sku_id_type1_101_count")).select("user_id", a + "sku_id_type1_101_count")
    val action_tmp_101_type2 = action_tmp_type_2.filter($"cate" === 101)
      .groupBy("user_id").agg(count("sku_id").as(a + "sku_id_type2_101_count")).select("user_id", a + "sku_id_type2_101_count")


    val feat_df = order_tmp_30And101.join(order_tmp_30, Seq("user_id"), "outer")
      .join(broadcast(order_tmp_101), Seq("user_id"), "outer")
      .join(broadcast(order_tmp_other), Seq("user_id"), "outer")
      .join(broadcast(order_tmp_firstDay), Seq("user_id"), "outer")
      .join(broadcast(action_tmp_30And101), Seq("user_id"), "outer")
      .join(broadcast(action_tmp_30And101_type1), Seq("user_id"), "outer")
      .join(broadcast(action_tmp_30And101_type2), Seq("user_id"), "outer")
      .join(broadcast(action_tmp_30), Seq("user_id"), "outer")
      .join(broadcast(action_tmp_30_type1), Seq("user_id"), "outer")
      .join(broadcast(action_tmp_30_type2), Seq("user_id"), "outer")
      .join(broadcast(action_tmp_101), Seq("user_id"), "outer")
      .join(broadcast(action_tmp_101_type1), Seq("user_id"), "outer")
      .join(broadcast(action_tmp_101_type2), Seq("user_id"), "outer").na.fill(0)
    df_label_joined.join(broadcast(feat_df), Seq("user_id"), "left").na.fill(0)

  }
}