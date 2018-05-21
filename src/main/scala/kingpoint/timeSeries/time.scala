package kingpoint.timeSeries

import java.text.SimpleDateFormat
import java.time.{ZoneId, ZonedDateTime}
import java.util.Calendar
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2017/4/20.
 */
object time {

  def main(args: Array[String]) {
//    val startTime="200305"
//    val endTime="201607"
//    val predictedN=5
//    //形成开始start到预测predicted的日期
//    var dateArrayBuffer=new ArrayBuffer[String]()
//    val dateFormat= new SimpleDateFormat("yyyyMM");
//    val cal1 = Calendar.getInstance()
//    val cal2 = Calendar.getInstance()
//
//    //设置训练数据中开始和结束日期
//    cal1.set(startTime.substring(0,4).toInt,startTime.substring(4).toInt,0)
//    cal2.set(endTime.substring(0,4).toInt,endTime.substring(4).toInt,0)
//
//    //开始日期和预测日期的月份差
//    val monthDiff = (cal2.getTime.getYear() - cal1.getTime.getYear()) * 12 +( cal2.getTime.getMonth() - cal1.getTime.getMonth())+predictedN
//    var iMonth=0
//    while(iMonth<=monthDiff){
//      //日期加1个月
//      cal1.add(Calendar.MONTH, iMonth)
//      //保存日期
//      dateArrayBuffer+=dateFormat.format(cal1.getTime)
//      cal1.set(startTime.substring(0,4).toInt,startTime.substring(4).toInt,0)
//      iMonth=iMonth+1
//    }
//    println(dateArrayBuffer.toArray)

    predictedDay()

  }

  def predictedDay(): Unit ={
    val startTime="20030501"
    val endTime="20141201"
    val predictedN=5
    //形成开始start到预测predicted的日期
    var dayArrayBuffer=new ArrayBuffer[String]()
    val dateFormat= new SimpleDateFormat("yyyyMMdd");
    val cal1 = Calendar.getInstance()
    val cal2 = Calendar.getInstance()

    //设置训练数据中开始和结束日期
    cal1.set(startTime.substring(0,4).toInt,startTime.substring(4,6).toInt-1,startTime.substring(6).toInt)
    cal2.set(endTime.substring(0,4).toInt,endTime.substring(4,6).toInt-1,endTime.substring(6).toInt)

    //开始日期和预测日期的月份差
    val dayDiff = (cal2.getTimeInMillis-cal1.getTimeInMillis)/ (1000 * 60 * 60 * 24)
    var iDay=0
    while(iDay<=dayDiff){
      //日期加1天
      cal1.add(Calendar.DATE, iDay)
      //保存日期
      dayArrayBuffer+=dateFormat.format(cal1.getTime)
      cal1.set(startTime.substring(0,4).toInt,startTime.substring(4,6).toInt-1,startTime.substring(6).toInt)
      iDay=iDay+1
    }
    println(dayArrayBuffer.toArray.mkString(","))

    val dt=ZonedDateTime.of(2003,11,1,0,0,0,0,ZoneId.systemDefault())
    println(dt)
  }
}
