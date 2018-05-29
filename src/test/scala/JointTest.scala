import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Administrator on 2018/5/28.
  */
object JointTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
      .master("local[*]")
      .getOrCreate()
import spark.implicits._

   val reader= spark.read.option("header", "true")
      .option("nullValue", "NA")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
    val basePath = "E:\\dataset\\JData_UserShop\\"

    val order=reader.csv(basePath+"jdata_user_order.csv")
    val user=reader.csv(basePath+"jdata_user_basic_info.csv")
    val data=user.join(order,Seq("user_id"),"outer")
    data.printSchema()
    data.show(false)
      data.select("age")
              .show()
  }
}
