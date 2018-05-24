package org.lzy.kaggle.JData

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}

import com.cloudera.sparkts.{DateTimeIndex, DayFrequency, TimeSeriesRDD, UniformDateTimeIndex}
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/5/22.
  */
object TimeSeriesTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
      .master("local[*]")
      .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
    val arr=Array((Timestamp.valueOf("2018-01-01 00:00:00"),"k1",0.5),
      (Timestamp.valueOf("2018-01-02 00:00:00"),"k2",0.7),
      (Timestamp.valueOf("2018-01-01 00:00:00"),"k1",0.9)
    )






    val df=spark.createDataFrame(arr).toDF("time","key","value")
    val zoneId = ZoneId.systemDefault()



    val dateArr=Array(      ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0,zoneId), ZonedDateTime.of(2018, 1, 2, 0, 0, 0, 0, zoneId), ZonedDateTime.of(2018, 1, 3, 0, 0, 0, 0,zoneId))
   val irregularTimeIndex= DateTimeIndex.irregular(dateArr)
    val timeIndex:UniformDateTimeIndex=DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0,zoneId),
      ZonedDateTime.of(2018, 1, 3, 0, 0, 0, 0, zoneId),
      new DayFrequency(1))

    val hybridTimdeIndex=DateTimeIndex.hybrid(Array(irregularTimeIndex,timeIndex))


    val ts_rdd=TimeSeriesRDD.timeSeriesRDDFromObservations(hybridTimdeIndex,df,"time","key","value")
    println(ts_rdd.collectAsTimeSeries().index.toZonedDateTimeArray().mkString(","))
    ts_rdd.foreach(println)
  }
}
