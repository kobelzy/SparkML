package org.lzy.kaggle.JData

import com.ml.kaggle.JData.TimeFuture.basePath
import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Auther: lzy
  * Description:
  * Date Created by： 9:17 on 2018/5/28
  * Modified By：
  */


object OrderAndActionCluster {
    val basePath = "E:\\dataset\\JData_UserShop\\"
    //  val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
    val sku = "jdata_sku_basic_info.csv"
    val user_basic = "jdata_user_basic_info.csv"
    val user_action = "jdata_user_action.csv"
    val user_order = "jdata_user_order.csv"
    val user_comment = "jdata_user_comment_score.csv"


        def main(args: Array[String]): Unit = {
            val spark = SparkSession.builder().appName("names")
                    .master("local[*]")
                    .getOrCreate()
            //        spark.sparkContext.setLogLevel("WARN")
            val orderAndActionCluster = new OrderAndActionCluster(spark)
            //商品信息,sku_id,price,cate,para_1,para_2,para_3
            val sku_df = orderAndActionCluster.getSourceData(basePath + sku)
            //用户信息,user_id,age,sex,user_lv_cd
            val user_df = orderAndActionCluster.getSourceData(basePath + user_basic).cache()
            //用户行为，user_id,sku_id,a_date,a_num,a_type
                val action_df=orderAndActionCluster.getSourceData(basePath+user_action)
            //订单表，user_id,sku_id,o_id,o_date,o_area,o_sku_num
            val order_df = orderAndActionCluster.getSourceData(basePath + user_order)
                    .cache()
            //评价表,user_id,comment_create_tm,o_id,score_level
            val comment_df = orderAndActionCluster.getSourceData(basePath + user_comment)

            val all_df=orderAndActionCluster.getUnionDF(order_df,action_df,user_df,sku_df)
            all_df.printSchema()
            all_df.show(false)
    }
}
class OrderAndActionCluster(spark:SparkSession) {

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

def getUnionDF(order_df:DataFrame,action_df:DataFrame,user_id:DataFrame,sku_df:DataFrame)={
   val order_new_df= order_df.select("user_id","sku_id","o_date","o_sku_num")
                    .toDF("o_user_id","o_sku_id","o_date","o_sku_num")
//    user_id,sku_id,a_date,a_num,a_type
    action_df.select("user_id","sku_id","a_date","a_num","a_type")
    //用户行为，user_id,sku_id,a_date,a_num,a_type,,,,join,,,,,user_id,age,sex,user_lv_cd

    val action$User$Sku_df=action_df.join(user_id,Seq("user_id"),"left")
            .map{ case Row(user_id:Int, sku_id:Int ,a_date:Timestamp,a_num:Int,a_type:Int,age:Option[Int],sex:Option[Int],user_lv_cd:Option[Int])=>
                Row(user_id,sku_id,a_date,a_num,a_type,user_lv_cd.getOrElse(0))
            }
            //商品信息,sku_id,price,cate,para_1,para_2,para_3
            .join(sku_df.select("sku_id","cate"),Seq("sku_id"),"left")
            .map{ case Row(user_id:Int, sku_id:Int ,a_date:Timestamp,a_num:Int,a_type:Int,user_lv_cd:Int,cate:Option[Int])=>
                Row(user_id,sku_id,a_date,a_num,a_type,user_lv_cd,cate.getOrElse(0))
            }
            .select("user_id","sku_id","a_date","a_num","a_type","user_lv_cd","cate")
    //需要通过用户以及o_date,a_date进行join，如果时间 相同，会被留下来
    //订单表，user_id,sku_id,o_id,o_date,o_area,o_sku_num
    val all_df= action$User$Sku_df.join(order_df,action$User$Sku_df("user_id")===order_new_df("o_user_id") and action$User$Sku_df("a_date")===order_new_df("o_date"),"outer")
                    .map{case Row(user_id:Option[Int], sku_id:Option[Int] ,a_date:Option[Timestamp],a_num:Option[Int],a_type:Option[Int],user_lv_cd:Option[Int],cate:Option[Int],
                    o_user_id:Option[Int],o_sku_id:Option[Int],o_date:Option[Timestamp],o_sku_num:Option[Int])=>
                        //通过数据选择，将两种数据分隔开来，这里优先保证订单中数据的存在，之后才能保证浏览数据的存在。
                        //如果订单的数据也存在，浏览数据也存在，那么会用订单，但是浏览类型我觉得需要去掉了。嗯，那么就需要分开进行判断了。
//                        //商品类别，需要用不同的。

                        o_user_id match {
                            case Some(id) =>(id,o_date.get,o_sku_id.get,0,0,user_lv_cd.get)
                        }
                        val user_id_all=o_user_id.getOrElse(user_id.get)
                        val date=o_date.getOrElse(a_date.get)
                        val sku_id_all=o_sku_id.getOrElse(sku_id.get)
                        val a_num_all=a_num.getOrElse(0)
                           val a_type_all=a_type.getOrElse(0)
                            val user_lv_cd_all=user_lv_cd.getOrElse(0)
                            val cate_all=cate.getOrElse(0)
                            val o_sku_num_all=o_sku_num.getOrElse(0)
                    }
//           .select("user_id","sku_id","a_date","a_num","a_type","user_lv_cd","cate","o_user_id","o_sku_id","o_date","o_sku_num")
    all_df
}

}

