import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2018/5/21.
  */
object ArrayTest {
  def main(args: Array[String]): Unit = {
//    val outData =  Array(1,2,3)
//    outData(1)=10
//    println(outData.mkString(","))
//
//    val ts=Vectors.dense(Array(10.1,10.2))
//    val dest=Vectors.dense(Array(20.1,20.2))
////    addTimeDependentEffects(ts,dest).toArray.foreach(println)
//
//    val destArr = dest.toArray
//    destArr(1)=1000
//    println("dest:"+dest.toArray.mkString(","))
//
//    val arr=ArrayBuffer(10,100)
//    val desArr=arr.toArray
//    desArr(1)=1000
//    println("arr:"+arr.toArray.mkString(","))
//
//
//val map=Map(1->1,2->2)
//    println(map)
//  }
//
//  def addTimeDependentEffects(ts: Vector, dest: Vector): Vector = {
//    val destArr = dest.toArray
//    println("dest:"+dest.toArray.mkString(","))
//
//    val fitted = Array(100,200)
//    for (i <- 0 to (dest.size - 1)) {
//      destArr(i) = fitted(i)
//    }
//    println("dest:"+dest.toArray.mkString(","))
//    dest

//    val arr=Array(("1",1),("2",2)).reverse
//    println(arr.sortBy(_._2).mkString(","))
//    println(math.log1p(100000))
//    println(math.exp(math.log1p(100000)-1))
//    println(math.exp(math.log1p(100000)+1))


    val arr=Array(0,1,2,3,4,5,6,7,8,9)
    val list=Array(0,1,2,3,4,5,6,7,8,9)
    println(arr.slice(0,3).mkString(","))
    println(arr.slice(5,arr.length).mkString(","))

    arr.update(2,10)
    println(arr.take(3).mkString(","))
    println(arr.mkString(","))

    println(arr.zipWithIndex.mkString("|"))
//    arr.formatted()
    arr.tails.foreach(s=> println(s.mkString(",")))
    println(arr.splitAt(3)._1.mkString(","))
    println(arr.splitAt(3)._2.mkString(","))
    println(arr.partition(_ / 2 == 0)._1.mkString(","))
    println(arr.partition(_ / 2 == 0)._2.mkString(","))

    println(arr.nonEmpty)
    println(Nil.isEmpty)
//    arr.update(5,10)
//    for (elem <- arr.sliding(3,4)) {print(elem.mkString("(",",",")"))}

    println(arr.drop(5).mkString(","))
    println(arr.dropRight(5).mkString(","))
    println(arr.dropWhile(_ / 2 != 0).mkString(","))
    println(arr.indexOfSlice(Array(3,4)))
    println(arr.indexWhere(_ / 2 == 3,8))
    println(arr.toIndexedSeq.mkString(","))
//    arr.inits.foreach(s=> println(s.mkString(",")))


//      arr.combinations(3).foreach(x=>println(x.mkString("_")))
//    println(arr.hasDefiniteSize)
//    println(arr.isDefinedAt(3))
//    println(arr.isDefinedAt(10))
    println(arr.lengthCompare(10))
    println(arr.lengthCompare(5))
    println(arr.lengthCompare(20))
    println(arr.maxBy(_>0))

    println(arr.partition(_ % 2.0 == 0)._1.mkString(","))
    println(arr.partition(_ % 2.0 == 0)._2.mkString(","))
    val lag=4
    println(arr.drop(lag+2).mkString(","))
    println(arr.dropRight(lag+2).mkString(","))

    println(arr.slice(0, arr.length - lag - 2).mkString(","))
    println(arr.slice(lag + 2, arr.length).mkString(","))
  }
}
