package org.lzy.Test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/9/4.
  */
object AnhuiTest {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("安徽测试")
    val sc=new SparkContext(conf)
    val data=sc.textFile("E:\\dataset\\本地网\\data")
    val splites=data.map(_.split(",",-1))
    val len_not_59=splites.filter(_.length!=39)
    len_not_59.foreach(line=>println(line.mkString(",")))
    println("不符合的长度为："+len_not_59.count())
  }
}
