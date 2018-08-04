package org.lzy.kaggle.kaggleSantander

import common.FeatureUtils
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.regression.{GBTRegressor, RandomForestRegressor}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

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
    def selectFeaturesByRF_evaluate(df: DataFrame, numOfFeatures: Int = 100): Array[String] = {
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
        val rf = new RandomForestRegressor().setSeed(10)
                .setLabelCol(Constant.lableCol)
        val rf_model = rf.fit(train)
        val feaImp_arr = rf_model.featureImportances.toArray
        val feaImp2Col_arr = feaImp_arr.zip(featureColumns_arr).sortBy(_._1).reverse.take(numOfFeatures)
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

    /** *
      * 功能实现:将数据分桶
      * zero:0.0   low:1.0   hight :2.0  top:3.0
      * Author: Lzy
      * Date: 2018/7/10 18:46
      * Param: [df, inputCol, outputCol]
      * Return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
      */
    def bucketFeatures(inputCol: String) = {

        val splits = Array(Double.NegativeInfinity, 0.1, math.pow(10, 5), math.pow(10, 6), math.pow(10, 7), Double.PositiveInfinity)
        val bucketizer = new Bucketizer()
                .setInputCol(inputCol)
                .setOutputCol(inputCol + "_bucket")
                .setSplits(splits)
                .setHandleInvalid("skip")
        bucketizer
    }

    val bucketizer_udf = udf { column: Double =>
        if (column <= 0) 0d
        else if (column > 0 && column <= math.pow(10, 5)) 1d
        else if (column > math.pow(10, 5) && column <= math.pow(10, 6)) 2d
        else if (column > math.pow(10, 6) && column <= math.pow(10, 7)) 3d
        else 4d
    }

    /** *
      * 功能实现:将制定列进行分桶，并将分桶后的记过进行assmble为outputCol名称
      *
      * Author: Lzy
      * Date: 2018/7/10 18:59
      * Param: [df, columns]
      * Return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
      */
    def featureBucketzer(df: DataFrame, columns: Array[String], outputCol: String) = {
        val assambleColumns = columns.take(2)
                .map(_ + "_bucket")
        var stages = Array[PipelineStage]()
        //var buckt_df=df
        columns.take(2).foreach(column => {
            stages = stages :+ bucketFeatures(column)
            //            buckt_df=buckt_df.withColumn(column,bucketizer_udf(col(column)))
        })
        stages = stages ++ (FeatureUtils vectorAssemble(assambleColumns, outputCol))
        val pipline = new Pipeline().setStages(stages)

        pipline.fit(df).transform(df)
    }

    def addStatitic(df: DataFrame) = {
        val columns = df.columns.filterNot(column => (Constant.featureFilterColumns_arr :+ "df_type").contains(column.toLowerCase()))
        val column_count = columns.length
        val median_index = (column_count / 2.0).toInt

        val df_rdd = df.select((col("id") +: (columns.map(column => col(column).cast(DoubleType)))): _*).rdd

                .map(row => {
                    var arr = Array[Double]()
                    for (i <- columns.indices) {
                        arr = arr :+ row.getDouble(i + 1)
                    }
                    val id = row.getString(0)
                    (id, arr)
                })
        val statistic_df = df_rdd.map { case (id, arr) => {
            val sum = arr.sum
            val mean = sum / column_count
            val std = math.sqrt(arr.map(x => math.pow(x - mean, 2)).sum)
            val nans = arr.count(x => x == 0 || x == 0d)
            val sort_arr = arr.sorted
            val median = sort_arr(median_index)
            (id, sum, mean, std, nans, median)
        }
        }.toDF("id", "sum", "mean", "std", "nans", "median")
        //        statistic_df.coalesce(10).write.mode(SaveMode.Overwrite).parquet(Constant.basePath+"cache/statistic_df")
        statistic_df.coalesce(10).write.mode(SaveMode.Overwrite).parquet(Constant.basePath + "cache/evaluate_statistic_df")
        statistic_df.show()
        statistic_df
    }

    def a(train: DataFrame, test: DataFrame) = {
        val all_df = FeatureUtils.concatTrainAndTest(train, test, Constant.lableCol)
        val columns = train.columns.filterNot(column => (Constant.featureFilterColumns_arr :+ "df_type" ++ Constant.cols_with_onlyone_val).contains(column.toLowerCase()))
        val train_df = train.select("id", columns :+ "target": _*)
        val rfFeatures_arr = selectFeaturesByRF(train_df, 1000)
        val rfFeatures_columns = rfFeatures_arr.map(column => col(column).cast(DoubleType))
        val train_count = train.count()
        val test_count = test.count()
        train_df.select(rfFeatures_columns: _*).rdd.map(row => {
            //计算每一行的每个值是否为0，不为0则统计一个值
            for (index <- rfFeatures_columns.indices) {
                row.getDouble(index) == 0d
            }
        })

    }


    def getBueautifulTest(train: DataFrame, test: DataFrame) = {
        val columns = test.columns.filterNot(column => Constant.featureFilterColumns_arr.contains(column.toLowerCase()))
        val round_columns = col("id").cast("String") +: columns.map(column => round(col(column), 2))
        val test_round_df = test.select(round_columns: _*)

        val df2RDD = { (df: DataFrame, columns: Array[String]) =>
            df.map(row => {
                val id = row.getString(0)
                var arr = Array[Double]()
                for (i <- columns.indices) {
                    arr = arr :+ row.getDouble(i + 1)
                }
                (id, arr)
            }).rdd
        }


        val test_rdd = df2RDD(test, columns)
        val test_round_rdd = df2RDD(test_round_df, columns)
        val id2isugly_rdd = test_rdd.join(test_round_rdd).map { case (id, (arr1, arr2)) =>
            val isEqual = arr1.sameElements(arr2)
            (id, isEqual)
        }
        //是否是真行
        val ugly_indexes = id2isugly_rdd.filter(_._2 == false).map(_._1).collect()
        val non_ugly_indexes = id2isugly_rdd.filter(_._2 == true).map(_._1).collect()

        val isNonUgly_udf = udf { (id: String) => non_ugly_indexes.contains(id) }
        val targetMean = train.select($"target").map(_.getDouble(0)).rdd.mean()
        val nonUgly_test_df = test.filter(isNonUgly_udf($"id"))
                .withColumn("target", lit(targetMean))
        println("真行数:" + nonUgly_test_df.count())
        nonUgly_test_df.write.mode(SaveMode.Overwrite).parquet(Constant.basePath + "cache/nonUgly_test_df")
        (nonUgly_test_df, non_ugly_indexes, ugly_indexes)
    }

    /** *
      * 功能实现:获取泄漏数据
      *
      * Author: Lzy
      * Date: 2018/7/23 18:52
      * Param: [df, columns, lag, predName]
      * Return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
      */
    def fastGetLeak(df: DataFrame, columns: Array[String], lag: Int = 0, predName: String) = {
        //获取开头到直到lag再往前两个值的columns
        val d1 = df.select((columns.dropRight(lag + 2) :+ "id").map(col): _*)
        //获取lag开始后两个之后的值的columns，并将lag加入
        val d2 = df.select(columns.drop(lag + 2).map(col) :+ col(columns(lag)).alias(predName): _*)
        //将需要添加的数据进行汇总，为一个向量
        val assmbel_d1 = FeatureUtils.vectorAssembl(columns.dropRight(lag + 2), "key").transform(d1)
        val assmbel_d2 = FeatureUtils.vectorAssembl(columns.drop(lag + 2), "key").transform(d2)
        //需要按照key值进行一下去重
        val d3 = assmbel_d2.dropDuplicates("key")
        val result = assmbel_d1.join(d3, Seq("key"), "left").select("id", predName).na.fill(0d)
        result
    }

    def compiledLeadResult(train: DataFrame) = {
        val cols = Constant.specialColumns_arr
        val transact_cols = train.columns.filterNot(column => Constant.featureFilterColumns_arr.contains(column.toLowerCase))
        val max_nlags = cols.length - 2

        //获取每一行的log的平均值
        val id2nonZero_mean_df: DataFrame = train.select(col("id") +: transact_cols.map(col(_).cast(DoubleType)): _*)
                .map(row => {
                    val id = row.getString(0)
                    val means: Double = transact_cols.indices.map(i => {
                        val x = row.getDouble(i + 1)
                        if (x == 0) 0 else math.log1p(x)
                    })
                            .sum / transact_cols.length
                    (id, math.expm1(means))
                }).toDF("id", "nonzero_mean").cache()
        id2nonZero_mean_df.show()
        var trainLeak_df = train.select((Array("id", "target") ++ cols).map(col): _*)
                //不生效，会熔断,放了前边就没有事？？？？？
                .withColumn("compiled_leak", lit(0d))
                .join(broadcast(id2nonZero_mean_df), "id").persist(StorageLevel.MEMORY_AND_DISK)
        trainLeak_df.show(false)
        var leaky_cols = Array[String]()
        var leaky_value_counts = Array[Long]()
        var leaky_value_corrects = Array[Double]()
        var scores = Array[Double]()

        def getRMSE = udf { (y: Double, tmp: Double) => math.pow(y - tmp, 2) }

        for (i <- 0 until max_nlags) {
            val c = "leaked_target_" + i
            println("processing lag:" + i)
            val fast_leak_df=fastGetLeak(trainLeak_df, cols, i, c)
            trainLeak_df = trainLeak_df.join(broadcast(fast_leak_df), "id").cache()
            leaky_cols = leaky_cols :+ c
            trainLeak_df = train.join(trainLeak_df.select("id", leaky_cols :+ "compiled_leak" :+ "nonzero_mean": _*), Seq("id"), "left").cache()
            println(2, "train_leak")
//      trainLeak_df.withColumn("compiled_leak",when($"compiled_leak" ===0d,col(c) ))
            trainLeak_df = trainLeak_df.withColumn("compiled_leak",when($"compiled_leak" ===0d,col(c) ).otherwise($"compiled_leak"))
            trainLeak_df.cache()
            leaky_value_counts = leaky_value_counts :+ trainLeak_df.filter($"compiled_leak" > 0).count()
            val _correct_counts = trainLeak_df.filter($"compiled_leak" === $"target").count()
            leaky_value_corrects = leaky_value_corrects :+ _correct_counts / leaky_value_counts.last.toDouble
            println("当前总泄漏数量为：", leaky_value_counts.last)
            println("泄漏成功占比：", leaky_value_corrects.last)

            val score_rdd = trainLeak_df
                            .withColumn("compiled_leak",when($"compiled_leak" ===0,log1p($"nonzero_mean")).otherwise(log1p($"compiled_leak")))
                    .na.fill(14.49, Seq("compiled_leak"))
                    .withColumn("y", log1p("target"))
                    .withColumn("score", getRMSE($"y", $"compiled_leak")).select("score").rdd.map(_.getDouble(0))
            scores = scores :+ math.sqrt(score_rdd.mean())
            println("当前的均方误差根为：" + scores.last)
            println("------------------------------")
            println(leaky_value_counts.mkString(","))
            println(leaky_value_corrects.mkString(","))
            println(scores.mkString(","))
            println("------------------------------")


            //      trainLeak_df.show()
        }
        (trainLeak_df, leaky_value_counts, leaky_value_corrects, scores)
    }

    /** *
      * 功能实现:使用泄漏值填充到compiled_leak中
      *
      * Author: Lzy
      * Date: 2018/7/24 10:27
      * Param: [leak_df, lag]
      * Return: java.lang.Object
      */
    def reWriteCompiledLeak(leak_df: DataFrame, lag: Int) = {
        var newLeak_df = leak_df
        newLeak_df = newLeak_df.withColumn("compiled_leak", lit(0d))
        (0 until lag).foreach(i => {
            val c = "leaked_target_" + i
            newLeak_df = newLeak_df.withColumn("compiled_leak", when($"compiled_leak" === 0d, col(c)).otherwise($"compiled_leak"))
        })
        newLeak_df
    }

    def compiledLeakResult_test(test: DataFrame, max_nlags: Int) = {
        val cols = Constant.specialColumns_arr
        val transact_cols = test.columns.filterNot(column => Constant.featureFilterColumns_arr.contains(column.toLowerCase))
        //获取每一行的log的平均值
        val id2nonZero_mean_df: DataFrame = test.select(col("id") +: transact_cols.map(col(_).cast(DoubleType)): _*)
                .map(row => {
                    val id = row.getString(0)
                    val means: Double = transact_cols.indices.map(i => {
                        val x = row.getDouble(i + 1)
                        if (x == 0) 0 else math.log1p(x)
                    })
                            .sum / transact_cols.length
                    (id, math.expm1(means))
                }).toDF("id", "nonzero_mean").cache()
        //        val test_df=test.withColumn("target",)
        var test_leak = test.select("id", Array("target") ++ Constant.specialColumns_arr: _*)
                .withColumn("compiled_leak", lit(0d))
                .join(broadcast(id2nonZero_mean_df), "id").persist(StorageLevel.MEMORY_AND_DISK)
        test_leak.show(false)

        var leaky_cols = Array[String]()
        var leaky_value_counts = Array[Long]()
//        var scores = Array[Double]()

//        def getRMSE = udf { (y: Double, tmp: Double) => math.pow(y - tmp, 2) }

        for (i <- 0 until max_nlags) {
            val c = "leaked_target_" + i
            println("processing lag:" + i + ",当前字段：" + c)

            test_leak = test_leak.join(fastGetLeak(test_leak, cols, i, c), "id")
            leaky_cols = leaky_cols :+ c
            test_leak = test.join(test_leak.select("id", leaky_cols :+ "compiled_leak" :+ "nonzero_mean": _*), Seq("id"), "left")
            println("test_leak")

            test_leak = test_leak.withColumn("compiled_leak",when($"compiled_leak" ===0d,col(c) ).otherwise($"compiled_leak"))
            test_leak.cache()
            leaky_value_counts = leaky_value_counts :+ test_leak.filter($"compiled_leak" > 0).count()
//            val _correct_counts = test_leak.filter($"compiled_leak" === $"target").count()
            println("在训练集中发现泄露数据：", leaky_value_counts.last)
            println(leaky_value_counts.mkString(","))

//            test_leak = test_leak.withColumn("compiled_leak", log1p($"nonzero_mean")).na.fill(14.49, Seq("nonzero_mean"))
//            val score_rdd = test_leak.withColumn("y", log1p("target"))
//                    .withColumn("score", getRMSE($"y", $"compiled_leak")).select("score").rdd.map(_.getDouble(0))
//            scores = scores :+ math.sqrt(score_rdd.mean())
//            println("当前分数，填充非0值之后的为：" + scores.last)
        }
        (test_leak, leaky_value_counts)
    }

}
