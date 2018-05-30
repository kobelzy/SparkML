package org.lzy.kaggle.JDataByLeaner

import java.sql.Timestamp

import kingpoint.timeSeries.time
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dmg.pmml.False
import spire.syntax.{action, order}
import org.apache.spark.sql.functions.col

/**
  * Created by Administrator on 2018/5/30.
  */


object FeaExact {
  val basePath = "E:\\dataset\\JData_UserShop\\"
  //  val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
      .master("local[*]")
      .getOrCreate()
    val util=new Util(spark)
    val (order,action)=util.loadData(basePath)
  }



}

class FeaExact(spark:SparkSession){
  import spark.implicits._
  def createFeat(startTime:Timestamp,endTime:Timestamp,user_df:DataFrame,
                 order:DataFrame,action:DataFrame,test:Boolean=false): Unit ={
    val order_df=order.sort($"o_date")
    val action_df=action.sort($"a_date")
    //预测目标月份的数据
    val label_month:Int=endTime.toLocalDateTime.getMonthValue

    //计算order和action与预测月份之间的时间差值。
    order_df.withColumn("day_gap",order_df.select($"o_date".as[Timestamp]).map(date=>(endTime.getTime-date.getTime)/(60*60*24*1000)).col("o_date"))
    action_df.withColumn("day_gap",action_df.select($"a_date".as[Timestamp]).map(date=>(endTime.getTime-date.getTime)/(60*60*24*1000)).col("a_date"))
    action_df.select($"a_date".as[Timestamp]).map(date=>(endTime.getTime-date.getTime)/(60*60*24*1000))

 if(test){
   val df_label=user_df.withColumn("label_1",col("age")*0-1)
     .withColumn("label_2",col("age")*0-1)
 }else{
   //找到用户在目标月份最早的订单日期
   val order_label_1=order_df.filter(order_df("o_date") === label_month)
     .filter(order_df("cate") === 30 || order_df("cate")===101)
   val label=order_label_1.sort("o_date").dropDuplicates("user_id")
   val order_label=user_df.join(label.select("user_id","o_date"),Seq("user_id"),"left")

   //label_1代表用户在目标月份购买的订单个数，label_2代表用户在一个月的几号下的订单

 }


 }
}