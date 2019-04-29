package org.lzy.scalaTest

import org.apache.spark.util.collection.AppendOnlyMap
import org.scalatest.FunSuite

import scala.collection.mutable.HashSet

//ScalaTest提供了名为FunSuite的特质重载了execute方法从而可以用函数值的方式，而不是方法定义测试
class SetSuite extends FunSuite {
  /**
    * test后面圆括号（）内为测试名称，可以使用任何字符串，不需要传统的驼峰形式命名。
    * ！！！！！！！但是名称需要唯一！！！！！！！！！！！
    * 圆括号后面的大括号{}之内定义的为测试代码。被作为传名参数传递给test函数，由test登记备用
    */
  test("An empty Set should have size 0") {
    assert(Set.empty.size == 0)
  }
  test("Invoking head on an empty Set should produce NoSuchElementException") {
    assertThrows[NoSuchElementException] {
      //intercept[NoSuchElementException] {  旧版本使用，现在仍可用
      Set.empty.head
    }
  }
    test("object keys and values") {
      val map = new AppendOnlyMap[String, String]()
      for (i <- 1 to 100) {
        map("" + i) = "" + i
      }
      assert(map.size === 100)
      for (i <- 1 to 100) {
        assert(map("" + i) === "" + i)
      }
      assert(map("0") === null)
      assert(map("101") === null)
      assert(map(null) === null)
      val set = new HashSet[(String, String)]
      for ((k, v) <- map) {   // Test the foreach method
        set += ((k, v))
      }
      assert(set === (1 to 100).map(_.toString).map(x => (x, x)).toSet)
    }


}
