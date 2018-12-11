package org.lzy.kaggle.kaggleRisk

import common.{DataUtils}
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/7/7.
  */
object Run{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
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
    val utils = new DataUtils(spark)
    val run=new Run(spark)
run.mergionSub()
  }


}
class Run(spark:SparkSession) {
  import spark.implicits._
def mergionSub(): Unit ={
  val utils=new DataUtils(spark)
  val sub1=utils.read_csv("E:\\dataset\\Kggle_Risk\\submission_1.csv").toDF("SK_ID_CURR","TARGET1")
  val sub2=utils.read_csv("E:\\dataset\\Kggle_Risk\\submission_2.csv").toDF("SK_ID_CURR","TARGET2")

  sub1.show()
  val joined=sub1.join(sub2,"SK_ID_CURR")
println(sub1.count())
  println(sub2.count())
  println(joined.count())

 val result= joined.withColumn("result",($"TARGET1"+$"TARGET2")/2.0)
  result.show(false)
  val sub=result.select($"SK_ID_CURR",$"result".alias("TARGET"))
  utils.to_csv(sub,"E:\\dataset\\Kggle_Risk\\submission.csv")
}

}
