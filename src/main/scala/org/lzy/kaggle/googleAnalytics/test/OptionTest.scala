package org.lzy.kaggle.googleAnalytics.test

import java.text.SimpleDateFormat

import scala.util.Try

object OptionTest {
  def main(args: Array[String]): Unit = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val sdf2 = new SimpleDateFormat("yyyyMMdd hh:mm:dd")
    val str = "20180101"
    val str2 = "20180101 10:00:00"
    val date: Option[Long] = Try(sdf.parse(str2).getTime).toOption
    println(date)
    val date2: Option[Long] = Try(sdf2.parse(str2).getTime)
      .orElse(Try(sdf.parse(str2).getTime))
      .toOption
    println(date2)


    val op: Option[Int] = None
    val op2:Option[Int] = op.map(_ + 1)
    println(op2==null)
  }
}
