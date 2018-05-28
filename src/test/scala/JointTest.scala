import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/5/28.
  */
object JointTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("names")
      .master("local[*]")
      .getOrCreate()


   val reader= spark.read.option("header", "true")
      .option("nullValue", "NA")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
    val basePath = "E:\\dataset\\JData_UserShop\\"

    val order=reader.csv(basePath+"jdata_user_order_test.csv")
    val user=reader.csv(basePath+"jdata_user_basic_info_test.csv")
    val data=order.join(user,Seq("user_id"),"outer")
    data.printSchema()
    data.show(false)
  }
}
