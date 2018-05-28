import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2018/5/21.
  */
object ArrayTest {
  def main(args: Array[String]): Unit = {
    val outData =  Array(1,2,3)
    outData(1)=10
    println(outData.mkString(","))

    val ts=Vectors.dense(Array(10.1,10.2))
    val dest=Vectors.dense(Array(20.1,20.2))
//    addTimeDependentEffects(ts,dest).toArray.foreach(println)

    val destArr = dest.toArray
    destArr(1)=1000
    println("dest:"+dest.toArray.mkString(","))

    val arr=ArrayBuffer(10,100)
    val desArr=arr.toArray
    desArr(1)=1000
    println("arr:"+arr.toArray.mkString(","))



  }

  def addTimeDependentEffects(ts: Vector, dest: Vector): Vector = {
    val destArr = dest.toArray
    println("dest:"+dest.toArray.mkString(","))

    val fitted = Array(100,200)
    for (i <- 0 to (dest.size - 1)) {
      destArr(i) = fitted(i)
    }
    println("dest:"+dest.toArray.mkString(","))
    dest
  }
}
