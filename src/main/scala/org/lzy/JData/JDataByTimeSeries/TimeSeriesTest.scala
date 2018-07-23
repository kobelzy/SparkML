package org.lzy.JData.JDataByTimeSeries

import com.cloudera.sparkts.models.{ARIMA, ARIMAModel, GARCH}
import com.cloudera.sparkts.{DateTimeIndex, DayFrequency, TimeSeriesRDD, UniformDateTimeIndex}
import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable

/**
  * Created by Administrator on 2018/5/22.
  */
object TimeSeriesTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
      .master("local[*]")
      .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
    val arr=Array(
      (Timestamp.valueOf("2018-01-01 00:00:00"),"k1",0.5),
      (Timestamp.valueOf("2018-01-02 00:00:00"),"k2",0.7),
      (Timestamp.valueOf("2018-01-03 00:00:00"),"k3",0.9),
      (Timestamp.valueOf("2018-01-02 00:00:00"),"k3",0.5),
      (Timestamp.valueOf("2018-01-03 00:00:00"),"k1",0.7),
      (Timestamp.valueOf("2018-01-01 00:00:00"),"k2",0.9)
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


    val ts_rdd=TimeSeriesRDD.timeSeriesRDDFromObservations(timeIndex,df,"time","key","value")
    val ts_fill_rdd=ts_rdd.fill("zero")
    df.show()
    ts_fill_rdd.foreach(println)
//    println(ts_rdd.collectAsTimeSeries().index.toZonedDateTimeArray().mkString(","))
    val foreach=arimaModelTrain(ts_fill_rdd,3)
    foreach.foreach(println)
  }


  def ARCHModelTrain[K](trainTsrdd: TimeSeriesRDD[K],predictedN:Int):RDD[Vector]={
    val archRDD=trainTsrdd.map{case (key,denseVector)=>
      (GARCH.fitModel(denseVector),denseVector)
    }
    /***预测出后N个的值*****/
    //构成N个预测值向量，之后导入到holtWinters的forcast方法中
    val predictedArrayBuffer=new mutable.ArrayBuffer[Double]()
    var i=0
    while(i<predictedN){
      predictedArrayBuffer+=i
      i=i+1
    }
    println(predictedArrayBuffer.toArray.mkString(","))
    val predictedVectors=Vectors.dense(predictedArrayBuffer.toArray)
    val foreast: RDD[Vector] =archRDD.map{case (model,vector)=>
      model.addTimeDependentEffects(vector,vector)
    }
    foreast
  }


  def arimaModelTrain[K](trainTsrdd: TimeSeriesRDD[K], predictedN: Int): RDD[Vector] = {
    /** *参数设置 ******/

    /** *创建arima模型 ***/
    //创建和训练arima模型.其RDD格式为(ArimaModel,Vector)
    val arimaAndVectorRdd: RDD[(ARIMAModel, Vector)] = trainTsrdd.map { case (key, denseVector) =>
      //      (ARIMA.autoFit(denseVector), denseVector)
      val naVector=Vectors.dense(Array[Double]())

      (ARIMA.autoFit(denseVector,1,0,1), naVector)

    }

    /** *预测出后N个的值 *****/
    val forecast = arimaAndVectorRdd.map { case (arimaModel, denseVector) => arimaModel.forecast(denseVector, predictedN)    }
    forecast
  }
}
