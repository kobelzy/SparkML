package org.lzy.kaggle.googleAnalytics.test

import java.text.SimpleDateFormat

import scala.util.Try

object OptionTest {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val sdf2 = new SimpleDateFormat("yyyyMMdd hh:mm:dd")
  def main(args: Array[String]): Unit = {
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

    run2()
  }

  def run2()={
    val str=Some("")
//    val result=str.fold(11L)(date=>sdf.parse(date).getTime)
//    println(result)

    println(Try(sdf.parse(str.getOrElse("")).getTime).toOption)
  }
}
