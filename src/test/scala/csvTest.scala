import org.apache.spark.sql.SparkSession
import org.lzy.kaggle.JDataByLeaner.FeaExact.basePath
import org.lzy.kaggle.JDataByLeaner.Util

/**
  * Created by Administrator on 2018/6/2.
  */
object csvTest {
  def main(args: Array[String]): Unit = {
    val basePath = "E:\\dataset\\JData_UserShop\\"
    val spark = SparkSession.builder().appName("names")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val util = new Util(spark)
    val order_cache = spark.read.parquet(basePath + "cache/order")
order_cache.show(false)
    order_cache.dropDuplicates("user_id").show(false)
  }
}
