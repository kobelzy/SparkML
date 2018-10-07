package scala.org.lzy.Test

/**
  * Created by Administrator on 2018/10/1.
  */
class ApplyTest(str: Option[String]) {
  def this(str2: String) = this(Option(str2))

  println(str.getOrElse("sss"))
}

object ApplyTest {
  def main(args: Array[String]): Unit = {
    //    ApplyTest("test")
    new ApplyTest("test2")
    val num: Option[String] = None
    println(num.map(_.toInt))
  }

  def apply(str: String) = {
    println(str + "apply")
  }

  def apply(str: Option[String]) = {
    println(str + "apply")
  }
}
