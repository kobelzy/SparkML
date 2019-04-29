package org.lzy.scalaTest

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class UsingBeforeAndAfter extends FlatSpec with BeforeAndAfter with Matchers{
var list:java.util.List[String]=_
  before{
    println("before")
    list=new java.util.ArrayList[String]
  }
  after{
    println("after")
    if(list!=null) list =null
  }
  "a list".should("be empty on create").in(list.size.shouldBe(0))
  it.should("increase in size upon add").in{
    list.add("Milk")
  list.add("Sugar")
  list.size.shouldBe(2)
  }
}
