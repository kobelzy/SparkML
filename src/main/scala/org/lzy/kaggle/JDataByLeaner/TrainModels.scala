package org.lzy.kaggle.JDataByLeaner

import org.apache.spark.ml.feature.{ChiSqSelector, ChiSqSelectorModel, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidatorModel, TrainValidationSplitModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by Administrator on 2018/6/3
  * spark-submit --master yarn-client --queue lzy --driver-memory 2g --conf spark.driver.maxResultSize=2g  \
  * --num-executors 12 --executor-cores 4 --executor-memory 7g --jars \
  * /root/lzy/xgboost/jvm-packages/xgboost4j-spark/target/xgboost4j-spark-0.8-SNAPSHOT-jar-with-dependencies.jar \
  * --class org.lzy.kaggle.JDataByLeaner.TrainModels SparkML.jar
  */
object TrainModels {

    //  val basePath = "E:\\dataset\\JData_UserShop\\"
    val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("names3")
                //            .master("local[*]")
                .getOrCreate()
        val sc = spark.sparkContext
        val config = sc.getConf
        //    config.set("spark.driver.maxResultSize","0")
        config.set("spark.debug.maxToStringFields", "100")
        config.set("spark.shuffle.io.maxRetries", "60")
        config.set("spark.default.parallelism", "54")
        config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        spark.sparkContext.setLogLevel("WARN")
        val util = new Util(spark)
        val trainModel = new TrainModels(spark, basePath)

        /*
        获取训练数据
         */
        val data04_df = spark.read.parquet(basePath + "cache/trainMonth/04")
        val data03_df = spark.read.parquet(basePath + "cache/trainMonth/03")
        val data02_df = spark.read.parquet(basePath + "cache/trainMonth/02")
        val data01_df = spark.read.parquet(basePath + "cache/trainMonth/01")
        val data12_df = spark.read.parquet(basePath + "cache/trainMonth/12")
        val data11_df = spark.read.parquet(basePath + "cache/trainMonth/11")
        val data10_df = spark.read.parquet(basePath + "cache/trainMonth/10")
        val data09_df = spark.read.parquet(basePath + "cache/trainMonth/09")

        val testTrain_df = data09_df.union(data10_df).union(data11_df).union(data12_df).union(data01_df).union(data02_df).union(data03_df).repartition(100)

        val featureCorr_list = trainModel.showFeatureLevel(testTrain_df, "label_2", Array("user_id", "label_1"))
        spark.createDataFrame(featureCorr_list).write.parquet(basePath + "corr/label2_corr")
    }
}

class TrainModels(spark: SparkSession, basePath: String) {
    val dropColumns: Array[String] = Array("user_id", "label_1", "label_2")
    val labelCol = "label_1"
    val predictCol = "o_num"
    val labelCol2 = "label_2"
    val predictCol2 = "pred_date"

    import spark.implicits._

    /*
    分数检验
     */
    def score(result_df: DataFrame) = {

        val udf_getWeight = udf { index: Int => 1.0 / (1 + math.log(index)) }
//      println("总数："+result_df.count())
        println("label_1预测结果大于0---->:" + result_df.filter($"o_num" > 0).count())
        println("label_1实际结果大于0---->:" + result_df.filter($"label_1" > 0).count())
//
        println("label_1预测结果小于0--->0:" + result_df.filter($"o_num" < 0).count())
        println("label_1实际结果小于0---->:" + result_df.filter($"label_1" < 0).count())
//    println("label_1实际结果等于0---->:"+result_df.filter($"label_1"===0).count())
//
//    println("label_2预测结果大于0---->:"+result_df.filter($"pred_date">0).count())
//    println("label_2实际结果大于0---->:"+result_df.filter($"label_2">0).count())
//    println("label_2预测结果小于0--->:"+result_df.filter($"pred_date"<0).count())
//    println("label_2实际结果小于0--->:"+result_df.filter($"label_2"<0).count())


        //按照label2预测的结果进行排序。
        val weight_df = result_df.coalesce(1).sort($"o_num".desc)
                .limit(50000)
                .withColumn("label_binary", when($"label_1" > 0, 1.0).otherwise(0.0))
                //      .withColumn("label_binary", udf_binary($"label_1"))
                .withColumn("index", monotonically_increasing_id + 1)
                .withColumn("weight", udf_getWeight($"index"))
        println("之后总数：" + weight_df.count())
        weight_df.show(false)
        //        val sorted = weight_df.sort($"pred_date")
//        println("最大值--->:")
//        weight_df.sort($"pred_date").show(false)
//        println("最小值--->:")
//        weight_df.sort($"pred_date".desc).show(false)
val s1 = weight_df.select($"label_binary".as[Double], $"weight".as[Double]).map(tuple => tuple._1 * tuple._2).collect().sum / 4674.32357
        //1 to 50000 map(i=>1.0/(1+math.log(i)))
        //计算s2
        val weightEqual1_df = result_df.filter($"label_1" > 0)
        val s2 = weight_df.filter($"label_1" > 0).select($"label_2".as[Double], $"pred_date".as[Double]).collect().map { case (label_2, pred_date) =>
            10.0 / (math.pow(label_2 - math.round(pred_date) + 1, 2) + 10)
        }.sum / weightEqual1_df.count().toDouble
        println(s"s1 score is $s1 ,s2 score is $s2 , S is ${0.4 * s1 + 0.6 * s2}")
    }


    /** *
      * 获取最终的计算结果。
      *
      * @return
      */
    def getResult(dataType: String = "test", test: DataFrame) = {
        //    val test = spark.read.parquet(basePath + s"cache/${dataType}_test_start12")
        val featureColumns: Array[String] = test.columns.filterNot(dropColumns.contains(_))
        val vectorAssembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features1")
        val test_df = vectorAssembler.transform(test)

        val chiSelector1_model = ChiSqSelectorModel.read.load(basePath + s"selector/s1_chiSelector")
        val chiSelector2_model = ChiSqSelectorModel.read.load(basePath + s"selector/s2_chiSelector")
        val test_select_df1 = chiSelector1_model.transform(test_df)
        val test_select_df2 = chiSelector2_model.transform(test_df)

        val s1_Model = TrainValidationSplitModel.read.load(basePath + s"model/s1_${dataType}_Model").bestModel
        val s2_Model = TrainValidationSplitModel.read.load(basePath + s"model/s2_${dataType}_Model").bestModel

        /**
          * 交叉验证方式
          */
//    val s1_Model = CrossValidatorModel.read.load(basePath + s"model/s1_${dataType}_ModelByCross").bestModel
//    val s2_Model = CrossValidatorModel.read.load(basePath + s"model/s2_${dataType}_ModelByCross").bestModel


val s1_df = s1_Model.transform(test_select_df1.withColumnRenamed(labelCol, "label"))
        .withColumnRenamed("label", labelCol)
        .withColumnRenamed("prediction", predictCol)
        .select("user_id", labelCol, predictCol)
        val s2_df = s2_Model.transform(test_select_df2.withColumnRenamed(labelCol2, "label"))
                .withColumnRenamed("label", labelCol2)
                .withColumnRenamed("prediction", predictCol2)
                .select("user_id", labelCol2, predictCol2)

        val result = s1_df.join(s2_df, "user_id")

        val udf_predDateToDate = udf { (pred_date: Double) => s"2017-05-${math.round(pred_date)}" }

        println(result.filter($"pred_date" > 0).count)
        println(result.filter($"pred_date" < 0).count)
        val submission: DataFrame = result
//      .filter($"pred_date"> -30.5 && $"pred_date" <0)
                .sort($"o_num".desc).limit(50000)
                .withColumn("result_date", udf_predDateToDate($"pred_date"))
                .select($"user_id", to_date($"result_date").as("pred_date"))
        submission.show(20, false)
        val errorData = submission.filter($"pred_date".isNull)
        errorData.join(result, "user_id").show(false)
        println("数据不合法" + errorData.count())
        println("结果数量：" + submission.count())
        println("最大：")
        println(submission.sort($"pred_date".desc).head().getDate(1))
        submission.coalesce(1).write
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                //          .option("nullValue", "NA")
                .csv(basePath + "sub/result")
        submission
    }


    /** *
      * 功能实现:     训练并保存数据
      * *dataType为test或者vali
      *
      * Author: Lzy
      * Date: 2018/6/13 9:17
      * Param: [dataType, train, round]
      * Return: void
      */
    def trainAndSaveModel(dataType: String = "test", train: DataFrame, round: Int,topNumFeatures:Int=200) = {

        val featureColumns: Array[String] = train.columns.filterNot(dropColumns.contains(_))
        val vectorAssembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features1")
        val train_df = vectorAssembler.transform(train)
        val (chiSelector1_Model, chiSelector2_Model) =
        //如果是验证集，那么需要进行卡方选择，
            if (dataType.equals("vali")) {
                val chiSelector1 = new ChiSqSelector().setOutputCol("features").setFeaturesCol("features1").setLabelCol(labelCol).setNumTopFeatures(topNumFeatures)
                val chiSelector1_Model = chiSelector1.fit(train_df)
                chiSelector1_Model.write.overwrite().save(basePath + s"selector/s1_chiSelector")

                val chiSelector2 = new ChiSqSelector().setOutputCol("features").setFeaturesCol("features1").setLabelCol(labelCol2).setNumTopFeatures(topNumFeatures)
                val chiSelector2_Model = chiSelector2.fit(train_df)
                chiSelector2_Model.write.overwrite().save(basePath + s"selector/s2_chiSelector")
                (chiSelector1_Model, chiSelector2_Model)
            } else {
                val chiSelector1_Model = ChiSqSelectorModel.read.load(basePath + s"selector/s1_${dataType}_chiSelector")
                val chiSelector2_Model = ChiSqSelectorModel.read.load(basePath + s"selector/s2_${dataType}_chiSelector")
                (chiSelector1_Model, chiSelector2_Model)

            }

        val train_selector1_df = chiSelector1_Model.transform(train_df)
        val train_selector2_df = chiSelector2_Model.transform(train_df)


        //为resul通过label_1来计算 添加o_num列，
        val s1_Model: TrainValidationSplitModel = Model.fitPredict(train_selector1_df, labelCol, predictCol, round)
        s1_Model.write.overwrite().save(basePath + s"model/s1_${dataType}_Model")
        //为result通过label_2来计算添加pred_date
        val s2_Model = Model.fitPredict(train_selector2_df, labelCol2, predictCol2, round)
        s2_Model.write.overwrite().save(basePath + s"model/s2_${dataType}_Model")

        /**
          * 交叉验证方式
          */
        //为resul通过label_1来计算 添加o_num列，
/*    val s1_Model = Model.fitPredictByCrossClassic(train_df, "label_1", "o_num",round)
//    val s1_Model = Model.fitPredictByCross(train_df, "label_1", "o_num",round)
    s1_Model.write.overwrite().save(basePath + s"model/s1_${dataType}_ModelByCross")
    //为result通过label_2来计算添加pred_date
    val s2_Model = Model.fitPredictByCross(train_df, "label_2", "pred_date",round)
    s2_Model.write.overwrite().save(basePath + s"model/s2_${dataType}_ModelByCross")*/
    }


    /** *
      * 功能实现:
      * 检验模型准确性
      * Author: Lzy
      * Date: 2018/6/13 9:16
      * Param: [dataType, test]
      * Return: void
      */
    def varifyModel(dataType: String = "test", test: DataFrame) = {
        //    val test = spark.read.parquet(basePath + s"cache/${dataType}_test_start12")
        val featureColumns: Array[String] = test.columns.filterNot(dropColumns.contains(_))
        val vectorAssembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features1")
        val test_df = vectorAssembler.transform(test)


        val test_select_df1 = ChiSqSelectorModel.read.load(basePath + s"selector/s1_chiSelector").transform(test_df)
        val test_select_df2 = ChiSqSelectorModel.read.load(basePath + s"selector/s2_chiSelector").transform(test_df)


        val s1_Model = TrainValidationSplitModel.read.load(basePath + s"model/s1_${dataType}_Model").bestModel
        val s2_Model = TrainValidationSplitModel.read.load(basePath + s"model/s2_${dataType}_Model").bestModel


        /**
          * 交叉验证方式
          */
        //    val s1_Model = CrossValidatorModel.read.load(basePath + s"model/s1_${dataType}_ModelByCross").bestModel
        //    val s2_Model = CrossValidatorModel.read.load(basePath + s"model/s2_${dataType}_ModelByCross").bestModel


        //训练测试数据并修改列名
        val s1_df = s1_Model.transform(test_select_df1.withColumnRenamed(labelCol, "label"))
                .withColumnRenamed("label", labelCol)
                .withColumnRenamed("prediction", predictCol)
                .select("user_id", labelCol, predictCol)
        val s2_df = s2_Model.transform(test_select_df2.withColumnRenamed(labelCol2, "label"))
                .withColumnRenamed("label", labelCol2)
                .withColumnRenamed("prediction", predictCol2)
                .select("user_id", labelCol2, predictCol2)

        val result = s1_df.join(s2_df, "user_id")
        //评分
        score(result)
    }

    /** *
      * 功能实现:计算多列的斯皮尔曼相关系数
      *
      * Author: Lzy
      * Date: 2018/6/14 15:19
      * Param: [df, label, noComputeColumn]
      * Return: scala.collection.immutable.List<scala.Tuple2<java.lang.String,java.lang.Object>>
      */
    def showFeatureLevel(df: DataFrame, label: String, noComputeColumn: Array[String]) = {
        val columns = df.columns.filterNot(column => (noComputeColumn :+ label).contains(column))
        var column2Corr_arr = List[(String, Double)]()
        for (column <- columns) {
            val corr = df.stat.corr(label, column)
            column2Corr_arr = column2Corr_arr :+ column -> corr
        }
        column2Corr_arr.sortBy(_._2)
    }


/*    def trainValiModel(dataType: String = "test", train: DataFrame, round: Int) = {

        //    val dropColumns1: Array[String] = Array("user_id", "label_1", "label_2","sex","sku_id_30_101_nunique","o_date_30_101_mean","o_month_30_101_nunique")
        val dropColumns1: Array[String] = Array("user_id", "label_1", "label_2")
        //    val dropColumns2: Array[String] = Array("user_id", "label_1", "label_2","sex","o_date_30_101_mean","OD90_o_date_30_101_nunique","OD180_o_date_30_101_nunique")
        val dropColumns2: Array[String] = Array("user_id", "label_1", "label_2")

        //    val train = spark.read.parquet(basePath + s"cache/${dataType}_train_start12")
        val featureColumns1: Array[String] = train.columns.filterNot(dropColumns1.contains(_))
        val featureColumns2: Array[String] = train.columns.filterNot(dropColumns2.contains(_))
        val selecter1 = new VectorAssembler().setInputCols(featureColumns1).setOutputCol("features1")
        val selecter2 = new VectorAssembler().setInputCols(featureColumns2).setOutputCol("features1")

        val train_df1 = selecter1.transform(train)
        val train_df2 = selecter2.transform(train)

        val chiSelector1 = new ChiSqSelector().setOutputCol("features").setFeaturesCol("features1").setLabelCol(labelCol).setNumTopFeatures(200)
        val chiModel1 = chiSelector1.fit(train_df1)
        chiModel1.write.overwrite().save(basePath + s"selector/s1_chiSelector")
        val train_selector1_df = chiModel1.transform(train_df1)
        val chiSelector2 = new ChiSqSelector().setOutputCol("features").setFeaturesCol("features1").setLabelCol(labelCol2).setNumTopFeatures(200)
        val chiModel2 = chiSelector2.fit(train_df2)
        chiModel2.write.overwrite().save(basePath + s"selector/s2_chiSelector")
        val train_selector2_df = chiModel2.transform(train_df2)

        //为resul通过label_1来计算 添加o_num列，
        val s1_Model: TrainValidationSplitModel = Model.fitPredict(train_selector1_df, labelCol, predictCol, round)
        s1_Model.write.overwrite().save(basePath + s"model/s1_${dataType}_Model")
        //为result通过label_2来计算添加pred_date
        val s2_Model = Model.fitPredict(train_selector2_df, labelCol2, predictCol2, round)
        s2_Model.write.overwrite().save(basePath + s"model/s2_${dataType}_Model")
    }

    def varifyValiModel(dataType: String = "test", test: DataFrame) = {
        //    val dropColumns1: Array[String] = Array("user_id", "label_1", "label_2","sex","sku_id_30_101_nunique","o_date_30_101_mean","o_month_30_101_nunique")
        val dropColumns1: Array[String] = Array("user_id", "label_1", "label_2")
        //    val dropColumns2: Array[String] = Array("user_id", "label_1", "label_2","sex","o_date_30_101_mean","OD90_o_date_30_101_nunique","OD180_o_date_30_101_nunique")
        val dropColumns2: Array[String] = Array("user_id", "label_1", "label_2")

        val featureColumns1: Array[String] = test.columns.filterNot(dropColumns1.contains(_))
        val featureColumns2: Array[String] = test.columns.filterNot(dropColumns2.contains(_))
        val selecter1 = new VectorAssembler().setInputCols(featureColumns1).setOutputCol("features1")
        val selecter2 = new VectorAssembler().setInputCols(featureColumns2).setOutputCol("features1")
        val test_df1 = selecter1.transform(test)
        val test_df2 = selecter2.transform(test)

        val chiModel1 = ChiSqSelectorModel.read.load(basePath + s"selector/s1_chiSelector")
        val chiModel2 = ChiSqSelectorModel.read.load(basePath + s"selector/s2_chiSelector")
        val test_select_df1 = chiModel1.transform(test_df1)
        val test_select_df2 = chiModel2.transform(test_df2)

        val s1_Model = TrainValidationSplitModel.read.load(basePath + s"model/s1_${dataType}_Model").bestModel
        val s2_Model = TrainValidationSplitModel.read.load(basePath + s"model/s2_${dataType}_Model").bestModel


        val s1_df = s1_Model.transform(test_select_df1.withColumnRenamed(labelCol, "label"))
                .withColumnRenamed("label", labelCol)
                .withColumnRenamed("prediction", predictCol)
                .select("user_id", labelCol, predictCol)
        val s2_df = s2_Model.transform(test_select_df2.withColumnRenamed(labelCol2, "label"))
                .withColumnRenamed("label", labelCol2)
                .withColumnRenamed("prediction", predictCol2)
                .select("user_id", labelCol2, predictCol2)

        val result = s1_df.join(s2_df, "user_id")
        score(result)
    }*/
}
