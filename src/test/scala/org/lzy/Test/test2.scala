package org.lzy.Test

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

/**
  * Created by Administrator on 2018/5/30.
  */
object test2 {
//    val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
    val basePath = "E:\\dataset\\JData_UserShop\\"

    def main(args: Array[String]): Unit = {

    //    println(getTime())
val spark = SparkSession.builder().appName("names")
        .master("local[*]")
        .getOrCreate()
        val reader= spark.read.option("header", "true")
                .option("nullValue", "NA")
                .option("inferSchema", "true")
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
    val data=reader.csv(basePath+"jdata_user_basic_info_test.csv")
    data.show(false)
//    data.printSchema()
//    println(data.count())
//      val arr=List(   Row(Timestamp.valueOf("2018-01-11 11:11:11")),
//     Row(Timestamp.valueOf("2018-01-14 11:11:11")))
//      case class datas(date:Timestamp)
      data.withColumn("index",monotonically_increasing_id+1).show(false)
  }
  def getTime()={
    val time=Timestamp.valueOf("2018-01-11 11:11:11")
    val time2=Timestamp.valueOf("2018-01-14 11:11:11")
//    timeSeries.time.toLocalDateTime.getDayOfMonth
//    timeSeries.time.toLocalDateTime.getYear

  }
}
