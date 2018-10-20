package org.lzy.kaggle.googleAnalytics.test

object FormatTest {
  def main(args: Array[String]): Unit = {
    println(10.100235454.formatted("%.4f"))
    println("%.4f".format(10.10035354))
  }
}
