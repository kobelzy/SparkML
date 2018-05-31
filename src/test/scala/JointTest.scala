import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, Row, SparkSession}

/**
  * Created by Administrator on 2018/5/28.
  */
object JointTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
      .master("local[*]")
      .getOrCreate()
import spark.implicits._
spark.sparkContext.setLogLevel("WARN")
   val reader= spark.read.option("header", "true")
      .option("nullValue", "NA")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
    val basePath = "E:\\dataset\\JData_UserShop\\"

    val order=reader.csv(basePath+"jdata_user_order_test.csv")
    val user=reader.csv(basePath+"jdata_user_basic_info_test.csv")
    val data=user.join(order,Seq("user_id"),"outer")
//    data.printSchema()
    data.show(false)
      data.na.fill(0).show(false)
//      data.select("age")
//              .show()
//    println(user.count)
//    user.filter($"user_id"===1).show()
//val arr=Array("age")
//     val newDF= user.groupBy(arr.head,arr.tail:_*).count().select("count",arr:_*).withColumn("newColumn",col("count")*0+1)
//      newDF.show()

//      val pivot:DataFrame=user.groupBy(arr.head,arr.tail:_*).pivot("age").count()
//      pivot.show()
  }
}
