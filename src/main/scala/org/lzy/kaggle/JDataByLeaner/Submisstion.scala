package org.lzy.kaggle.JDataByLeaner

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Auther: lzy
  * Description:
  * Date Created by： 9:29 on 2018/6/8
  * Modified By：
  */

object Submisstion {
    val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("names")
                .master("local[*]")
                .getOrCreate()
        import spark.implicits._
        val data = spark.read.parquet((basePath + "sub/result_parquet"))
                .filter($"pred_date"+0.49+1 <31 )
//        println(data.filter($"pred_date"+0.49+1 <30 ).count())
        val submission: DataFrame = data.sort($"o_num".desc)
                .withColumn("result_date", udfs($"pred_date"))
                .select("user_id", "result_date")

        submission.coalesce(1).write
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .option("nullValue", "NA")
                .csv(basePath + "sub/result")
    }

//    val udfs = udf { (pred_date: Double) => {
//        val days: Int = (1 + math.round(pred_date + 0.49 - 1)).toInt
//        getTime(s"2017-05-${days}")
//        val timestamp = Timestamp.valueOf("2017-05-01 00:00:00")
//        timestamp.setTime(timestamp.getTime + 1000 * 60 * 60 * 24 * days)
//    }
//    }
val udfs=udf{(pred_date:Double)=>{
    val days=(pred_date+0.49+1).toInt
    println(days)
    getTime(s"2017-05-${days}")
}}
    def getTime(yyyy_MM_dd: String) = {
        val date = yyyy_MM_dd + " 00:00:00"
        println(date)
        val dates = Date.valueOf("2017-05-01")
        val timestampe = Timestamp.valueOf(date)
        timestampe
    }
}
