package org.lzy.scalaTest

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class UsingGivenWhenThen extends FlatSpec with GivenWhenThen with Matchers {
  trait EmptyArrayList {
    val list = new java.util.ArrayList[String]
  }

  "a list" should "be empty on create" in new EmptyArrayList {
    Given("a empty list")
    Then("list size should be 0")
    list.size shouldBe 0
  }

  it should "increase in size upon add" in new EmptyArrayList {
    Given("a empty list")

    When("add 2 elements")
    list.add("Milk")
    list add "Sugar"

    Then("list size should be 2")
    list.size should be(2)
  }
}