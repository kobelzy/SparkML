package org.lzy.Test

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by Administrator on 2018/8/5.
  */
object OptionTest {
  def main(args: Array[String]): Unit = {
    val a=100
    val b: Option[Int] = None
    val opt: Option[Int] = Option(a)
    val result: Boolean = opt.exists(_ == 100)
    println(opt.filter(_ == 100))
    //    val date=new Date("20171016")
    val sdf = new SimpleDateFormat("yyyyMMdd")

    val date = sdf.parse("20170728").getTime
    println(date)
    //1508083200000
    //1508151024
    val newdate = new Date(1508151024L * 1000)
    println(newdate)
  }
}
