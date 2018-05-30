package org.lzy.kaggle.JDataByLeaner

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.dmg.pmml.False
import spire.syntax.{action, order}

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
    val label_month=endTime.toLocalDateTime.getMonthValue



  }
}