package org.lzy.scalaTest

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class UsingBeforeAndAfterAll extends FlatSpec with BeforeAndAfterAll with Matchers {

  var list: java.util.List[String] = _

  override def beforeAll(): Unit = {
    println("before all...")
    list = new java.util.ArrayList[String]
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    println("after all...")
    if (list != null) {
      list = null
    }
    super.afterAll()
  }

  "a list" should "be empty on create" in {
    list.size shouldBe 0
  }

  it should "increase in size upon add" in {
    list.add("Milk")
    list add "Sugar"

    list.size should be(2)
  }
}
