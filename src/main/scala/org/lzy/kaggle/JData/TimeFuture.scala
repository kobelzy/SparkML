package com.ml.kaggle.JData

import breeze.linalg.DenseVector
import com.cloudera.sparkts.models.{ARIMA, ARIMAModel}
import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import java.util
import com.cloudera.sparkts.{DateTimeIndex, IrregularDateTimeIndex, TimeSeries, TimeSeriesRDD}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.mllib.linalg.{DenseMatrix, Vector, Vectors, DenseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions.udf

/**
  * Created by Administrator on 2018/5/14.
  */
object TimeFuture {
//    val basePath = "E:\\dataset\\JData_UserShop\\"
    val basePath="hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
    val sku = "jdata_sku_basic_info.csv"
    val user_basic = "jdata_user_basic_info.csv"
    val user_action = "jdata_user_action.csv"
    val user_order = "jdata_user_order.csv"
    val user_comment = "jdata_user_comment_score.csv"

    case class Sku_Case(sku_id: Int, price: Double, cate: Integer, para_1: Double, para_2: Int, para_3: Int)

    case class User_Case(user_id: Int, age: Int, sex: Int, user_lv_cd: Int)

    case class Order_Case(user_id: Int, sku_id: Int, o_id: Int, o_date: Timestamp, o_area: Int, o_sku_num: Int)

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("names")
//                .master("local[*]")
                .getOrCreate()
//        spark.sparkContext.setLogLevel("WARN")
        val timeFuture = new TimeFuture(spark)
        //商品信息,sku_id,price,cate,para_1,para_2,para_3
        val sku_df = timeFuture.getSourceData(basePath + sku)
        //用户信息,user_id,age,sex,user_lv_cd
        val user_df = timeFuture.getSourceData(basePath + user_basic).cache()
        //用户行为，user_id,sku_id,a_date,a_num,a_type
        //    val action_df=timeFuture.getSourceData(basePath+user_action)
        //订单表，user_id,sku_id,o_id,o_date,o_area,o_sku_num
        val order_df = timeFuture.getSourceData(basePath + user_order)
                .cache()
        //评价表,user_id,comment_create_tm,o_id,score_level
        val comment_df = timeFuture.getSourceData(basePath + user_comment)
        order_df.show(false)
        /**
          * 做关联,基于订单表
          */
        //            val order2user_df:DataFrame = order_df.join(user_df, "user_id")
        //            val order2user2sku_df=order2user_df.join(sku_df,"sku_id")
        //            TimeSeriesRDD.timeSeriesRDDFromObservations()
        val timeIndex: IrregularDateTimeIndex = timeFuture.createTimeIndexsByNanos(order_df)
//        println(timeFuture.createTimeIndexsByNanos(order_df).size)
        timeFuture.makeTrainRDD(order_df, timeIndex)
        //        joins.printSchema()
        //    println(user_df.count())
        //    println(order_df.select("user_id").distinct().count())
//        val result: RDD[((Int, Int), List[(Int, Timestamp, Int, Int, Array[Timestamp])])] = timeFuture.unionOrder2Action(order_df, user_df)
        //    result.foreach(tuple=>println(tuple._1,tuple._2,tuple._3,tuple._4,tuple._5,tuple._6.mkString(",")))
        //    result.foreach(tuple=>println(tuple._1,tuple._2,tuple._3,tuple._4,tuple._5,tuple._6.length))
        //    result.foreach(println)
    }
}

class TimeFuture(spark: SparkSession) {

    import spark.implicits._

    /**
      * 获取csv转换为DF
      *
      * @param path
      * @return
      */
    def getSourceData(path: String): DataFrame = {
        val data = spark.read.option("header", "true")
                .option("nullValue", "NA")
                .option("inferSchema", "true")
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .csv(path)
        data
    }

    def unionOrder2Action(order_df: DataFrame, action_df: DataFrame): RDD[(Int, Int, Int, Timestamp, Int, Int, Array[Timestamp], Array[(Timestamp, Int, Int)])] = {
        val order_columns = Array("user_id", "sku_id", "o_id", "o_date", "o_area", "o_sku_num")
        //订单表，user_id,sku_id,o_id,o_date,o_area,o_sku_num
        //    order_df.groupByKey(_.getAs[Int](0))
        //      .flatMapGroups{case Row(user_id:Int,sku_id:Int,o_id:Int,o_area:Int,o_sku_num:Int)=>
        //        (sku_id,o_id,o_area,o_sku_num)
        //      }
        val order_timeZone_rdd: RDD[((Int, Int), List[(Int, Timestamp, Int, Int, Array[Timestamp])])] = order_df.map { case Row(user_id: Int, sku_id: Int, o_id: Int, o_date: Timestamp, o_area: Int, o_sku_num: Int) => (user_id, (sku_id, o_id, o_date, o_area, o_sku_num)) }
                .rdd
                .groupByKey()
                .flatMap { case (user_id, iter) =>
                    val sku2other_list: List[(Int, (Int, Timestamp, Int, Int))] = iter.toList.map(tuple => {
                        val (sku_id, o_id, o_date, o_area, o_sku_num) = tuple
                        (sku_id, (o_id, o_date, o_area, o_sku_num))
                    })
                    //聚合到商品粒度
                    val sku2other_grouped_map: Map[Int, List[(Int, Timestamp, Int, Int, Array[Timestamp])]] = sku2other_list.groupBy(_._1)
                            .mapValues { sku2other_list =>
                                //根据时间戳进行排序
                                val sku2other_sorted_list = sku2other_list.sortBy(_._2._2.getTime)


                                var o_id2timeZone_list = mutable.ListBuffer[(Int, Timestamp, Int, Int, Array[Timestamp])]()
                                val lastIndex: Int = sku2other_sorted_list.size - 1
                                for (i <- sku2other_sorted_list.indices) {
                                    val (sku_id, (o_id, o_date, o_area, o_sku_num)) = sku2other_sorted_list(i)
                                    if (i == 0) {
                                        o_id2timeZone_list += Tuple5(o_id, o_date, o_area, o_sku_num, Array(sku2other_sorted_list.head._2._2))
                                    } else if (i == lastIndex) {
                                        o_id2timeZone_list += Tuple5(o_id, o_date, o_area, o_sku_num, Array(sku2other_sorted_list(i - 1)._2._2, o_date))
                                    } else {
                                        o_id2timeZone_list += Tuple5(o_id, o_date, o_area, o_sku_num, Array(sku2other_sorted_list(i - 1)._2._2, o_date))
                                    }
                                    //               i match {
                                    //                  //如果为0，那么是最小的时间戳，使用( ,t]格式
                                    //                  case 0 => o_id2timeZone_list += Tuple5(o_id, o_date, o_area, 0, Array(sku2other_sorted_list.head._2._2))
                                    //                  //如果是最后一个值，那么使用(t, )
                                    //                  case lastIndex =>o_id2timeZone_list += Tuple5(o_id, o_date, o_area, 50, Array(sku2other_sorted_list.last._2._2))
                                    //                  //那么使用(t-1,t]
                                    //                  case _ => o_id2timeZone_list +=Tuple5(o_id, o_date, o_area, 100, Array(sku2other_sorted_list(i - 1)._2._2, o_date))
                                    //                }
                                }
                                o_id2timeZone_list.toList
                            }
                    //扩充到用户粒度
                    //      val sku2other_timeZone_list= sku2other_grouped_list.flatMap{case (sku_id,o_id2timeZone_list)=>
                    //          o_id2timeZone_list.map{case (o_id, o_date, o_area, o_sku_num,arr)=>
                    //            (sku_id, o_date, o_area, o_sku_num,arr)
                    //          }
                    //        }
                    //扩充到order粒度
                    //        sku2other_grouped_map.map{case  (sku_id, o_date, o_area, o_sku_num,arr)=>(user_id,sku_id, o_date, o_area, o_sku_num,arr)}

                    sku2other_grouped_map.map(sku2other => {
                        val sku_id = sku2other._1
                        val list = sku2other._2
                        ((user_id, sku_id), list)
                    })
                }
        //用户行为，user_id,sku_id,a_date,a_num,a_type
        //每个用户每一天的浏览记录
        val action_rdd: RDD[((Int, Int), List[(Timestamp, Int, Int)])] = action_df.map { case Row(user_id: Int, sku_id: Int, a_date: Timestamp, a_num: Int, a_type: Int) => ((user_id, sku_id), (a_date, a_num, a_type)) }
                .rdd
                .groupByKey().mapValues(_.toList)
       val orderJoinAction_rdd= order_timeZone_rdd.leftOuterJoin(action_rdd)
                .map { case ((user_id, sku_id), (order_list, action_list_opt)) =>
                    //list1 (o_id, o_date, o_area, o_sku_num,arr)
                    //list2 (a_date,a_num,a_type)
                    //将list2中的a_date插入list1的arr范围中，遍历list1，子遍历list2.
                    // 如果时间在arr中，那么新加一个arr，将时间段放进去，以及a_num,a_type.
                    //可以将行为时间转化为和购买日期的差放进去
                    ///但是一个订单可能包含多个行为，怎么版？
                  val order2actionAssmbel_list:List[(Int, Timestamp, Int, Int, Array[Timestamp], Array[(Timestamp, Int, Int)])]=  action_list_opt match {
                        case None => order_list.map { case (o_id, o_date, o_area, o_sku_num, range_arr) => (o_id, o_date, o_area, o_sku_num, range_arr, Array[(Timestamp, Int, Int)]()) }
                        case Some(action_list) => {
                            order_list.map { case (o_id, o_date, o_area, o_sku_num, range_arr) =>
                                val action_arr = mutable.ArrayBuffer[(Timestamp, Int, Int)]()

                                range_arr.length match {
                                    case 1 => {
                                        for ((a_date, a_num, a_type) <- action_list) {
                                            if (a_date.before(range_arr.head)) action_arr += Tuple3(a_date, a_num, a_type)
                                        }
                                    }
                                    case 2 => {
                                        for ((a_date, a_num, a_type) <- action_list) {
                                            if (a_date.after(range_arr.head) && a_date.before(range_arr.last)) action_arr += Tuple3(a_date, a_num, a_type)
                                        }
                                    }
                                }
                                (o_id, o_date, o_area, o_sku_num, range_arr, action_arr.toArray)
                            }
                        }
                    }
                    (user_id,sku_id,order2actionAssmbel_list)
                }

        //扩充到Order粒度
        val orderJoinActionByOrder_rdd=orderJoinAction_rdd.flatMap{case (user_id,sku_id,order2actionAssmbel_list)=>
            order2actionAssmbel_list.map{case  (o_id, o_date, o_area, o_sku_num, range_arr, action_arr)=>
                (user_id,sku_id,o_id,o_date,o_area,o_sku_num,range_arr,action_arr)
            }

        }

        orderJoinActionByOrder_rdd
    }


    /* def unionOrder2Action2(order_df: DataFrame, action_df: DataFrame): RDD[(Int, Int, Timestamp, Int, Int, Array[Timestamp])] = {
         val order_columns = Array("user_id", "sku_id", "o_id", "o_date", "o_area", "o_sku_num")
         //订单表，user_id,sku_id,o_id,o_date,o_area,o_sku_num
         //    order_df.groupByKey(_.getAs[Int](0))
         //      .flatMapGroups{case Row(user_id:Int,sku_id:Int,o_id:Int,o_area:Int,o_sku_num:Int)=>
         //        (sku_id,o_id,o_area,o_sku_num)
         //      }
         val order_timeZone_rdd = order_df.map { case Row(user_id: Int, sku_id: Int, o_id: Int, o_date: Timestamp, o_area: Int, o_sku_num: Int) => (user_id, (sku_id, o_id, o_date, o_area, o_sku_num)) }
                 .rdd
                 .groupByKey()
                 .flatMap { case (user_id, iter) =>
                     val sku2other_list: List[(Int, (Int, Timestamp, Int, Int))] = iter.toList.map(tuple => {
                         val (sku_id, o_id, o_date, o_area, o_sku_num) = tuple
                         (sku_id, (o_id, o_date, o_area, o_sku_num))
                     })
                     //聚合到商品粒度
                     val sku2other_grouped_list: Map[Int, List[(Int, Timestamp, Int, Int, Array[Timestamp])]] = sku2other_list.groupBy(_._1)
                             .mapValues { sku2other_list =>
                                 //根据时间戳进行排序
                                 val sku2other_sorted_list = sku2other_list.sortBy(_._2._2.getTime)


                                 var o_id2timeZone_list = mutable.ListBuffer[(Int, Timestamp, Int, Int, Array[Timestamp])]()
                                 val lastIndex: Int = sku2other_sorted_list.size - 1
                                 for (i <- sku2other_sorted_list.indices) {
                                     val (sku_id, (o_id, o_date, o_area, o_sku_num)) = sku2other_sorted_list(i)
                                     if (i == 0) {
                                         o_id2timeZone_list += Tuple5(o_id, o_date, o_area, o_sku_num, Array(sku2other_sorted_list.head._2._2))
                                     } else if (i == lastIndex) {
                                         o_id2timeZone_list += Tuple5(o_id, o_date, o_area, o_sku_num, Array(sku2other_sorted_list(i - 1)._2._2, o_date))
                                     } else {
                                         o_id2timeZone_list += Tuple5(o_id, o_date, o_area, o_sku_num, Array(sku2other_sorted_list(i - 1)._2._2, o_date))
                                     }
                                     //               i match {
                                     //                  //如果为0，那么是最小的时间戳，使用( ,t]格式
                                     //                  case 0 => o_id2timeZone_list += Tuple5(o_id, o_date, o_area, 0, Array(sku2other_sorted_list.head._2._2))
                                     //                  //如果是最后一个值，那么使用(t, )
                                     //                  case lastIndex =>o_id2timeZone_list += Tuple5(o_id, o_date, o_area, 50, Array(sku2other_sorted_list.last._2._2))
                                     //                  //那么使用(t-1,t]
                                     //                  case _ => o_id2timeZone_list +=Tuple5(o_id, o_date, o_area, 100, Array(sku2other_sorted_list(i - 1)._2._2, o_date))
                                     //                }
                                 }
                                 o_id2timeZone_list.toList
                             }
                     //扩充到用户粒度
                     val sku2other_timeZone_list = sku2other_grouped_list.flatMap { case (sku_id, o_id2timeZone_list) =>
                         o_id2timeZone_list.map { case (o_id, o_date, o_area, o_sku_num, arr) =>
                             (sku_id, o_date, o_area, o_sku_num, arr)
                         }
                     }
                     //扩充到order粒度
                     sku2other_timeZone_list.map { case (sku_id, o_date, o_area, o_sku_num, arr) => (user_id, sku_id, o_date, o_area, o_sku_num, arr) }
                 }
         //用户行为，user_id,sku_id,a_date,a_num,a_type
         //每个用户每一天的浏览记录
         //  action_df.map{case Row(user_id:Int,sku_id:Int,a_date:Timestamp,a_num:Int,a_type:Int)=>(user_id,(sku_id,a_date,a_num,a_type))}
         //  .rdd
         order_timeZone_rdd
     }
 */

    def createTimeIndexs(data: DataFrame): IrregularDateTimeIndex = {
        //  val zoneId=ZoneId.systemDefault()
        val dt_arr = data.map { row =>
            val o_date: Timestamp = row.getTimestamp(3)
            val o_date_str = o_date.toString
            o_date_str
        }.collect()
                .map(o_date_str => {
                    val zoneId = ZoneId.systemDefault()
                    ZonedDateTime.of(o_date_str.substring(0, 4).toInt, o_date_str.substring(5, 7).toInt, o_date_str.substring(8, 10).toInt, 0, 0, 0, 0, zoneId)
                })

        val irregularDTI: IrregularDateTimeIndex = DateTimeIndex.irregular(dt_arr)
        irregularDTI
    }


    def createTimeIndexsByNanos(data: DataFrame): IrregularDateTimeIndex = {
        val zoneId = ZoneId.systemDefault()
        val dt_arr = data.map { row =>
            val o_date: Timestamp = row.getTimestamp(3)
            o_date.getTime * 1000000
        }.collect()
        val irregularDTI: IrregularDateTimeIndex = DateTimeIndex.irregular(dt_arr)
        irregularDTI
    }

    val getStrColumn = udf { line: Int => line.toString }

    def makeTrainRDD(df: DataFrame, dateTimeIndex: DateTimeIndex) = {
        //先用order——df来做一次测试
        //订单表，user_id,sku_id,o_id,o_date,o_area,o_sku_num

        val assembler = new VectorAssembler()
                .setInputCols(Array("sku_id", "o_area", "o_sku_num"))
                .setOutputCol("feature")
        val train_df_Int = assembler.transform(df)
//       val train_df= train_df_Int.withColumn("user_id_str",getStrColumn($"user_id"))
//        train_df.show(false)
//        val timeSeriesRDD: TimeSeriesRDD[String] = TimeSeriesRDD.timeSeriesRDDFromObservations(dateTimeIndex, train_df, "o_date","user_id_str","feature")
//        val matricValues_arr=df.flatMap{case (user_id:Int,sku_id:Int,o_id:Int,o_date:Timestamp,o_area:Int,o_sku_num:Int)=>Array(o_area.toDouble,o_sku_num.toDouble)}.collect()
//        val matrix=new DenseMatrix(matricValues_arr.length/2,2,matricValues_arr)
//        val keys=df.select($"user_id".as[Int]).collect()
//        val timeSeries: TimeSeries[Int]= new TimeSeries[Int](dateTimeIndex,matrix,keys)


//        val user2feature_rdd:RDD[(Int, Vector)]=df.select("user_id","sku_id","o_area","o_sku_num")
//                .map{case Row(user_id:Int,sku_id:Int,o_date:Timestamp,o_area:Int,o_sku_num:Int)=>(user_id,Vectors.dense(Array(sku_id.toDouble,o_area.toDouble,o_sku_num.toDouble)))}.rdd

        val user2feature_rdd: RDD[(Int, Vector)] = train_df_Int.select("user_id", "feature").map { case Row(user_id: Int, feature: linalg.DenseVector) => (user_id, Vectors.dense(feature.toArray)) }.rdd
        val timeSeries_rdd = new TimeSeriesRDD[Int](dateTimeIndex, user2feature_rdd)
        timeSeries_rdd.take(10).foreach(println)
        val result: RDD[String] = arimaModelTrain(timeSeries_rdd, 30)
        result.take(10).foreach(println)
        println("长度：" + result.count())
        result.coalesce(1).saveAsTextFile("hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/output/testResult")
    }


    /**
      * Arima模型：
      * 输出其p，d，q参数
      * 输出其预测的predictedN个值
      *
      * @param trainTsrdd
      */
    def arimaModelTrain(trainTsrdd: TimeSeriesRDD[Int], predictedN: Int): RDD[String] = {
        /** *参数设置 ******/

        /** *创建arima模型 ***/
        //创建和训练arima模型.其RDD格式为(ArimaModel,Vector)
        val arimaAndVectorRdd: RDD[(ARIMAModel, Vector)] = trainTsrdd.map { line =>
            line match {
                case (key, denseVector) =>
                    (ARIMA.autoFit(denseVector), denseVector)
            }
        }

        //参数输出:p,d,q的实际值和其系数值
        val coefficients: RDD[(String, (Int, Int, Int))] = arimaAndVectorRdd.map { case (arimaModel, denseVector) => {
            (arimaModel.coefficients.mkString(","),
                    (arimaModel.p, arimaModel.d, arimaModel.q))
        }
        }
        coefficients.collect().map {
            _ match {
                case (coefficients, (p, d, q)) =>
                    println("coefficients:" + coefficients + "=>" + "(p=" + p + ",d=" + d + ",q=" + q + ")")
            }
        }

        /** *预测出后N个的值 *****/
        val forecast = arimaAndVectorRdd.map { row =>
            row match {
                case (arimaModel, denseVector) => {
                    arimaModel.forecast(denseVector, predictedN)
                }
            }
        }
        val forecastValue = forecast.map(_.toArray.mkString(","))

        //取出预测值
        val preditcedValueRdd: RDD[String] = forecastValue.map { parts =>
            val partArray = parts.split(",")
            for (i <- partArray.length - predictedN until partArray.length) yield partArray(i)
        }.map(_.toArray.mkString(","))
        preditcedValueRdd.collect().map { row =>
            println("forecast of next " + predictedN + " observations: " + row)
        }
        preditcedValueRdd
    }


}
