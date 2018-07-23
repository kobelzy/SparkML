package org.lzy.JData.JDataByTimeSeries

import com.cloudera.sparkts.models.{ARIMA, ARIMAModel}
import com.cloudera.sparkts.{DateTimeIndex, DayFrequency, TimeSeriesRDD, UniformDateTimeIndex}
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Auther: lzy
  * Description:
  * Date Created by： 18:37 on 2018/5/29
  * Modified By：
  */

object ARIMATrain{
//        val basePath = "E:\\dataset\\JData_UserShop\\"
    val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
    val formater=DateTimeFormatter.ofPattern("yyyy-MM-dd")
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("names")
//                .master("local[*]")
                .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        val all_df_path = basePath + "cache/kmeas_Result"
        val all_df_cache = spark.read.parquet(all_df_path).toDF("user_id","types","prediction","date")
        .selectExpr("user_id","double(prediction)","date")
        all_df_cache.show(false)
       val key2classic2date= train(all_df_cache,"date","user_id","prediction")
        val result=key2classic2date.filter(_._2>47).map(tuple=>tuple._1+","+tuple._3)
        result.take(10).foreach(println)
        result.coalesce(1).saveAsTextFile(basePath+"result/result0530")

    }


    def train(df:DataFrame,ts:String,key:String,value:String) ={
        val zoneId = ZoneId.systemDefault()
//        val timeIndex:UniformDateTimeIndex=DateTimeIndex.uniformFromInterval(
//            ZonedDateTime.of(2016, 5, 1, 0, 0, 0, 0,zoneId),
//            ZonedDateTime.of(2017, 4, 30, 0, 0, 0, 0, zoneId),
//            new DayFrequency(1))
        val timeIndex:UniformDateTimeIndex=DateTimeIndex.uniformFromInterval(
            ZonedDateTime.of(2017, 3, 1, 0, 0, 0, 0,zoneId),
            ZonedDateTime.of(2017, 4, 30, 0, 0, 0, 0, zoneId),
            new DayFrequency(1))
        val forcastTimeIndex:UniformDateTimeIndex=DateTimeIndex.uniformFromInterval(
            ZonedDateTime.of(2017, 5, 1, 0, 0, 0, 0,zoneId),
            ZonedDateTime.of(2017, 5, 31, 0, 0, 0, 0, zoneId),
            new DayFrequency(1))
        val forcastTimeArr=forcastTimeIndex.toZonedDateTimeArray()
                .map(_.format(formater))
        val timeSeries_rdd: TimeSeriesRDD[String] = TimeSeriesRDD.timeSeriesRDDFromObservations(timeIndex, df, ts,key,value)
                        .fill("zero")
                        .filter(_._2.toArray.filter(_==0.0).size<30)
        timeSeries_rdd.take(10).foreach(println)
       val key2foreast= arimaModelTrain(timeSeries_rdd,1)
        //选择其中value值满足阈值的数据，将其选出并为其转换日期。
        val key2classic2date_rdd=key2foreast.flatMap{case (k,vector)=>
            val arr=vector.toArray
            arr.map(classic=>{
                    (k,classic,forcastTimeArr(arr.indexOf(classic)))
                })
        }
        key2classic2date_rdd
    }

    /**
      * Arima模型：
      * 输出其p，d，q参数
      * 输出其预测的predictedN个值
      *
      * @param trainTsrdd
      */
    def arimaModelTrain[K](trainTsrdd: TimeSeriesRDD[K], predictedN: Int): RDD[(K, Vector)] = {
        /** *参数设置 ******/

        /** *创建arima模型 ***/
        //创建和训练arima模型.其RDD格式为(ArimaModel,Vector)
        val arimaAndVectorRdd: RDD[(K, ARIMAModel)] = trainTsrdd.map { case (key, denseVector) =>
//      (ARIMA.autoFit(denseVector), denseVector)
            (key,ARIMA.fitModel(1, 0, 1,denseVector))
//            (key,ARIMA.autoFit(denseVector))
        }
        /** *预测出后N个的值 *****/
        val key2Forcast = arimaAndVectorRdd.map { case (key, arimaModel) =>
            val naVector=Vectors.dense(Array[Double]())
           val forcast:Vector= arimaModel.forecast(naVector, predictedN)
            (key,forcast)
        }
        key2Forcast
    }
}
class ARIMATrain {

}
