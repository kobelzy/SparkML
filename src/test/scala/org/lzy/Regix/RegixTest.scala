package org.lzy.Regix

import scala.util.matching.Regex


/**
  * Created by Administrator on 2018/8/5.
  */
object RegixTest {
  def main(args: Array[String]): Unit = {
//      val pattern:Regex="(S|s)cala".r
//    val str="Scala is scalable and cool"
//    println(pattern.replaceAllIn(str,"Java"))
val str="4080084693999825737,\"8105:DT-CMYN-KM,SubNetwork=1,ManagedElement=51000000711697,EnbFunction=711697,EutranCellTdd=1\""
    println(str)
    val regex = "(\".*?),(.*?\")".r
    println(regex.replaceAllIn(str, "$1|$2"))
    println(regex.findAllIn(str).mkString("|"))
  }

}
