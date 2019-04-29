package org.lzy.scalaTest

import org.scalatest.{FlatSpec, Matchers}

class ScalaTestCheckException extends FlatSpec with Matchers {
  "string" should "throw IndexOutOfBoundsException when index is illegal" in {
  val s="test string"
    an [IndexOutOfBoundsException] should be thrownBy s.charAt(-1)
    val thrown=the [IndexOutOfBoundsException] thrownBy(s.charAt(-1))
    thrown.getMessage should( equal("String index out of range: -1"))
  }
}
