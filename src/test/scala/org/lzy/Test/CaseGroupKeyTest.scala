package org.lzy.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object CaseGroupKeyTest {
  case class data(id:String,name:String,age:Int)
  case class key(id:String,name:String)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("name")
      .master("local[*]")
      .getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("WARN")
import spark.implicits._
    val p=List(
      ("a","name1",10),
      ("b","name2",12),
      ("a","name1",15),
      ("b","name2",16)

    )
    val ds:Dataset[data]=spark.createDataFrame(p).toDF("id","name","age").as[data]
    ds.show(false)
val grouped:Dataset[(key, Int)]=ds.groupByKey(t=>key(t.id,t.name))
      .mapGroups{case (k,iter)=>
        (k,iter.size)
      }
    grouped.show(false)

    val rdd:RDD[(String, String, Int)]=sc.parallelize(p)
    rdd.groupBy(t=>key(t._1,t._2)).mapValues(_.size).foreach(println)
  }


}
