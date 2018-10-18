package org.lzy.kaggle.googleAnalytics

import java.text.SimpleDateFormat

import breeze.linalg.split
import common.SparkUtil

import scala.util.Try

object TransferDate {
  def main(args: Array[String]): Unit = {
    run(Constants.testPath)
  }
  def run(path:String)={
    val spark=SparkUtil.getSpark()
    val sc=spark.sparkContext
    val new_data=sc.textFile(path)
      .mapPartitions(iter=>{
        val sdf = new SimpleDateFormat("yyyyMMdd")
        iter.map(line=>{
          val splited=line.split(",")
          val date=sdf.parse(splited(1)).getTime.toString
          splited.update(1,date)
          splited.mkString(",")
        })
      })
      .coalesce(1)
      .saveAsTextFile(path+"_new")

  }
}
