import java.sql.Timestamp
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.lzy.JData.JDataByLeaner.FeaExact.basePath
import org.apache.spark.sql.functions._
import org.lzy.JData.JDataByLeaner.Util
/**
  * Created by Administrator on 2018/6/2.
  */
object csvTest {
  val sku = "jdata_sku_basic_info.csv"
  val user_basic = "jdata_user_basic_info_test.csv"
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
val order_df = util.getSourceData(basePath + user_order)
//val user_df = util.getSourceData(basePath + user_basic)
//    val df=user_df.join(order_df,Seq("user_id"),"left")
//        .sort($"o_date")
//    order_df.show(false)
//    order_df.dropDuplicates("user_id").show()
//    val index=1
//    val fun=udf{data:Int=>data}
//    val news=order_df.withColumn("index",fun(lit(index)))
//    news.show(false)
//    val fun2=udf((age:Int,index:Int)=>age+index)
//    order_df.withColumn("index",fun2($"o_id",lit(index))).show(false)
//    val startTime=Timestamp.valueOf("2017-01-01 00:00:00")
//    val s=df.withColumn("diff",datediff($"o_date",lit("2017-03-09 00:00:00")))
//    .withColumn("diff2",when($"o_sku_num" <=2,$"o_sku_num").otherwise(0))
//  .withColumn("label_2", when($"o_date".isNotNull && $"o_date" >= startTime , dayofmonth($"o_date")-1).otherwise(0)).sort("user_id")
//    s.show(false)
order_df.show(false)
//val new_order_df=order_df    .withColumn("o_sku_num",when($"o_sku_num" <=2,$"o_sku_num").otherwise(0))
//new_order_df.show(false)
//val scaler=new StandardScaler().setInputCol("o_sku_num")
//    scaler.fit(order_df).transform(order_df).show(false)

    val onehot=new OneHotEncoder().setInputCol("o_sku_num")
    onehot.transform(order_df).show(false)
  }
}
