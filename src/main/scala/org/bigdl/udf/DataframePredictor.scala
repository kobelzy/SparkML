package org.bigdl.udf

/**
  * Auther: lzy
  * Description:
  * Date Created by： 18:44 on 2018/6/12
  * Modified By：
  */

import com.intel.analytics.bigdl.example.utils.WordMeta
import com.intel.analytics.bigdl.utils.{Engine, LoggerFilter}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext

object DataframePredictor {

    LoggerFilter.redirectSparkInfoLogs()
    Logger.getLogger("com.intel.analytics.bigdl.example").setLevel(Level.INFO)

    def main(args: Array[String]): Unit = {

        Utils.localParser.parse(args, TextClassificationUDFParams()).foreach { param =>

            val conf = Engine.createSparkConf()
            conf.setAppName("Text classification")
                    .set("spark.task.maxFailures", "1")
            val sc = new SparkContext(conf)
            Engine.init

            // Create spark session
            val spark = new SQLContext(sc)
            import spark.implicits._

            var word2Meta = None: Option[Map[String, WordMeta]]
            var word2Index = None: Option[Map[String, Int]]
            var word2Vec = None: Option[Map[Float, Array[Float]]]

            val result = Utils.getModel(sc, param)

            val model = result._1
            word2Meta = result._2
            word2Vec = result._3
            val sampleShape = result._4

            // if not train, load word meta from file
            if (word2Meta.isEmpty) {
                val word2IndexMap = sc.textFile(s"${param.baseDir}/word2Meta.txt").map(item => {
                    val tuple = item.stripPrefix("(").stripSuffix(")").split(",")
                    (tuple(0), tuple(1).toInt)
                }).collect()
                word2Index = Some(word2IndexMap.toMap)
            } else {
                // already trained, use existing word meta
                val word2IndexMap = collection.mutable.HashMap.empty[String, Int]
                for((word, wordMeta) <- word2Meta.get) {
                    word2IndexMap += (word -> wordMeta.index)
                }
                word2Index = Some(word2IndexMap.toMap)
            }

            // if not train, create word vec
            if (word2Vec.isEmpty) {
                word2Vec = Some(Utils.getWord2Vec(word2Index.get))
            }
            val predict = Utils.genUdf(sc, model, sampleShape, word2Index.get, word2Vec.get)

            // register udf for data frame
            val classifierUDF = udf(predict)

            val data = Utils.loadTestData(param.testDir)

            val df = spark.createDataFrame(data)

            // static dataframe
            val types = sc.textFile(Utils.getResourcePath("/example/udfpredictor/types"))
                    .filter(!_.contains("textType"))
                    .map { line =>
                        val words = line.split(",")
                        (words(0).trim, words(1).trim.toInt)
                    }.toDF("textType", "textLabel")

            val classifyDF1 = df.withColumn("textLabel", classifierUDF($"text"))
                    .select("filename", "text", "textLabel")
            classifyDF1.show()

            val filteredDF1 = df.filter(classifierUDF($"text") === 9)
            filteredDF1.show()

            val df_join = classifyDF1.join(types, "textLabel")
            df_join.show()

            // aggregation
            val typeCount = classifyDF1.groupBy($"textLabel").count()
            typeCount.show()

            // play with udf in sqlcontext
            spark.udf.register("textClassifier", predict)
            df.registerTempTable("textTable")

            val classifyDF2 = spark
                    .sql("SELECT filename, textClassifier(text) AS textType_sql, text " +
                            "FROM textTable")
            classifyDF2.show()

            val filteredDF2 = spark
                    .sql("SELECT filename, textClassifier(text) AS textType_sql, text " +
                            "FROM textTable WHERE textClassifier(text) = 9")
            filteredDF2.show()
            sc.stop()
        }

    }

}