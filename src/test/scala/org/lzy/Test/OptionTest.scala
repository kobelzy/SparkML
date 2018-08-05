package org.lzy.Test

/**
  * Created by Administrator on 2018/8/5.
  */
object OptionTest {
  def main(args: Array[String]): Unit = {
    val a=100
    val opt: Option[Int] = Option(a)
    opt.exists(aobj=>aobj==100)
  }
}
