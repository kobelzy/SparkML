package org.lzy.Test

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, SparkSession}
/**
  * Created by Administrator on 2018/5/j28.
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

//    val seq: Double = (for (index <- 1 to 50000) yield 1.0 / (1 + math.log(index))).sum
//    val seq1=1 to 50000 map(i=>1.0/(1+math.log(i)))
//    println(seq1.size)
//    println(seq1.sum)

//    println(math.log(2.7))
//    println(math.log1p(1))
//    val endTime=Timestamp.valueOf("2017-01-01 00:00:00")
////    order.withColumn("news",datediff(lit(endTime),$"o_date")).show(false)
//      order                .withColumn("label_2", when($"o_date".isNotNull && $"o_date" >= endTime,dayofmonth($"o_date")-1).otherwise(0))
//              .show(false)
    order.show(false)
      val columns=order.columns
      order.select($"a".cast("double"))
order.select($"a".as[Double])
      val newTypeColumns:Array[Column]=columns.map(column=>col(column).cast(DoubleType))

//    order.withColumn("o_sku_num",when($"o_sku_num" >1,0 ).otherwise($"o_sku_num")).show(false)
//    order.withColumn("new",month($"o_date")).show(false)
//      order.selectExpr("month(o_date)").show(false)
////      order.select($"month(o_date)").show(false)
//      order.createOrReplaceTempView("order")
//      spark.sql("select month(o_date) from order").show(false)
//      val fea=new FeatureUtils(spark)
//val assemble=      FeatureUtils.vectorAssembl(Array("o_area","o_sku_num"),"features")
//      val df=assemble.transform(order)
//      val splits = Array(Double.NegativeInfinity, 100, Double.PositiveInfinity)
//      val bucketizer=new Bucketizer()
//              .setInputCol("o_area")
//              .setOutputCol("bucketedFeatures")
//              .setSplits(splits)
//      val bucketedData=bucketizer.transform(df)
//      bucketedData.show(false)
//      bucketedData.printSchema()
//      bucketedData.select("bucketedFeatures").distinct().show(false)
      println(math.pow(10,5))
 val arr=Array("o_area","o_sku_num")
      var buckt_df=order
      arr.foreach(column=>{
//            stages=stages:+bucketizer(column)
          buckt_df=buckt_df.withColumn(column,bucketizer_udf(col(column)))
      })
      buckt_df.show(false)
  }

    val bucketizer_udf=udf{column:Double=>
        if(column <=0) 0d
        else if(column >0 && column <=math.pow(10,5)) 1d
        else if(column >math.pow(10,5) && column <=math.pow(10,6)) 2d
        else if(column >math.pow(10,6) && column <=math.pow(10,7)) 3d
        else 4d
    }
}
