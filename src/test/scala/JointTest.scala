
/**
  * Created by Administrator on 2018/5/28.
  */
object JointTest {
  def main(args: Array[String]): Unit = {
    /*    val spark = SparkSession.builder().appName("names")
          .master("local[*]")
          .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
       val reader= spark.read.option("header", "true")
          .option("nullValue", "NA")
          .option("inferSchema", "true")
          .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
        val basePath = "E:\\dataset\\JData_UserShop\\"

        val order=reader.csv(basePath+"jdata_user_order_test.csv")*/

//    val seq: Double = (for (index <- 1 to 50000) yield 1.0 / (1 + math.log(index))).sum
//    val seq1=1 to 50000 map(i=>1.0/(1+math.log(i)))
//    println(seq1.size)
//    println(seq1.sum)

    println(math.log(2.7))
    println(math.log1p(1))
  }
}
