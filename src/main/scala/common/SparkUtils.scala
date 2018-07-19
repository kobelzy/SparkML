package common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
  * Created by Administrator on 2018/7/19.
  */
object SparkUtils {
  def getSpark(appName:String="names") ={
    val spark = SparkSession.builder().appName(appName)
                  .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val conf = spark.conf
    val sc = spark.sparkContext
    val config = sc.getConf
    //    config.set("spark.driver.maxResultSize","0")
    config.set("spark.debug.maxToStringFields", "100")
    config.set("spark.shuffle.io.maxRetries", "60")
    config.set("spark.default.parallelism", "54")
    config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark
  }

  def main(args: Array[String]): Unit = {
    val format_udf = udf { prediction: Double =>
      "%08.9f".format(prediction)
    }
    val spark=getSpark()
    import spark.implicits._
    val utils=new Utils(spark)
    val leak=utils.readToCSV("E:\\dataset\\Kaggle_Santander\\leak.csv").toDF("id","leak")
    val all=utils.readToCSV("E:\\dataset\\Kaggle_Santander\\s152.csv").toDF("id","all")
    println("all_count:"+all.count())
    val joined=all.join(leak,"id")
val result=joined.withColumn("target",when($"leak"===0.0,$"all").otherwise($"leak")).select($"id",format_udf($"target"))
result.show(false)
    utils.writeToCSV(result,"E:\\dataset\\Kaggle_Santander\\news.csv")
  }
}
