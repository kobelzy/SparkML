import org.apache.spark.sql.SparkSession
import org.lzy.kaggle.JDataByLeaner.FeaExact.basePath
import org.lzy.kaggle.JDataByLeaner.Util

/**
  * Created by Administrator on 2018/6/2.
  */
object csvTest {
  val sku = "jdata_sku_basic_info.csv"
  val user_basic = "jdata_user_basic_info.csv"
  val user_action = "jdata_user_action.csv"
  val user_order = "jdata_user_order_test.csv"
  val user_comment = "jdata_user_comment_score.csv"
  def main(args: Array[String]): Unit = {
    val basePath = "E:\\dataset\\JData_UserShop\\"
    val spark = SparkSession.builder().appName("names")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val util = new Util(spark)
//    val order_cache = spark.read.parquet(basePath + "cache/order")
//order_cache.show(false)
//    order_cache.dropDuplicates("user_id").show(false)
val order_df = util.getSourceData(basePath + user_order).sort($"o_date")
    order_df.show(false)
    order_df.dropDuplicates("user_id").show()
  }
}
