package org.lzy.JData.JDataByTimeSeries

import java.sql.Timestamp
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Auther: lzy
  * Description:
  * Date Created by： 9:17 on 2018/5/28
  * Modified By：
  *
  * spark-submit --master yarn-cluster --queue lzy  --num-executors 12 --executor-memory 7g --executor-cores 3 --class org.lzy.kaggle.JData.OrderAndActionCluster SparkML.jar
  */


object OrderAndActionCluster {
//    val basePath = "E:\\dataset\\JData_UserShop\\"
      val basePath = "hdfs://10.95.3.172:9000/user/lzy/JData_UserShop/"
    val sku = "jdata_sku_basic_info.csv"
    val user_basic = "jdata_user_basic_info.csv"
    val user_action = "jdata_user_action.csv"
    val user_order = "jdata_user_order.csv"
    val user_comment = "jdata_user_comment_score.csv"


    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("names")
//                .master("local[*]")
                .getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("WARN")
        val orderAndActionCluster = new OrderAndActionCluster(spark)

        //商品信息,sku_id,price,cate,para_1,para_2,para_3
        val sku_df = orderAndActionCluster.getSourceData(basePath + sku)
        //用户信息,user_id,age,sex,user_lv_cd
        val user_df = orderAndActionCluster.getSourceData(basePath + user_basic).cache()
        //用户行为，user_id,sku_id,a_date,a_num,a_type
        val action_df = orderAndActionCluster.getSourceData(basePath + user_action)
        //订单表，user_id,sku_id,o_id,o_date,o_area,o_sku_num
        val order_df = orderAndActionCluster.getSourceData(basePath + user_order).cache()
        //评价表,user_id,comment_create_tm,o_id,score_level
        val comment_df = orderAndActionCluster.getSourceData(basePath + user_comment)

        //合并数据
//        val all_df = orderAndActionCluster.getUnionDF(order_df, action_df, user_df, sku_df)
//        //预处理数据中间结果
val all_df_path = basePath + "cache/all_df"
//        all_df.write.mode(SaveMode.Overwrite).parquet(all_df_path)

        val all_df_cache = spark.read.parquet(all_df_path)
        //        val trains = all_df_cache.select("a_type", "a_num", "o_num", "user_lv_cd", "cate")       //其中a_num,o_num是连续变量
        val features_df = orderAndActionCluster.featureProcess(all_df_cache)
        features_df.show(false)
//        val kmeans_df=orderAndActionCluster.kmeansTrian(features_df)
//        for(k<-Array(10,20,30,40,50,60,70,80,90,100) ){
//            for (maxIter<- Array(10,20,30)) {
//                val kmeans_df = orderAndActionCluster.kmeansTrianByParam(features_df, k,maxIter)
//                val orderClass2Count_df = kmeans_df.select("types", "prediction").filter("types == 1").groupBy("prediction").count().sort("prediction")
//                orderClass2Count_df.show(50,false)
//                val arrs=orderClass2Count_df.map(_.getInt(0)).collect()
//                println("orderClass2Count_df的k数量：" + arrs.size)
//                println("means:"+ arrs.sum/arrs.size)
//            }
//        }


      val kmeans_df = orderAndActionCluster.kmeansTrianByParam(features_df, 100,30)
      kmeans_df.show(50,false)
      val action_arr = kmeans_df.filter("types == 0").select($"prediction".as[Int]).distinct().collect().sorted
      val actionIndex_map:Map[Int, Int]=action_arr.zipWithIndex.toMap.mapValues(_+1)
      val new_length=actionIndex_map.size
      println("action:"+actionIndex_map)
      println("action长度"+actionIndex_map.size)
      val order_arr = kmeans_df.filter("types == 1").select($"prediction".as[Int]).distinct().collect().sorted
      val orderIndex_map=order_arr.zipWithIndex.toMap.mapValues(_+new_length+1)
      println("orderIndex_map:"+orderIndex_map)
println("order长度："+orderIndex_map.size)
      val allMap=actionIndex_map++orderIndex_map
      println(allMap.mkString(","))
      println("长度："+allMap.size)
      val resultFuture_df=kmeans_df.select("user_id","types","prediction","date")
        .map{case Row(user_id:Int,types:Int,prediction:Int,date:Timestamp)=>
        val new_prediction=allMap(prediction)
          (user_id.toString,types,new_prediction,date)
        }
      resultFuture_df.write.parquet(basePath+"cache/kmeas_Result")
    }
}

class OrderAndActionCluster(spark: SparkSession) {

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

    def getUnionDF(order_df: DataFrame, action_df: DataFrame, user_id: DataFrame, sku_df: DataFrame) = {
        val order_new_df = order_df.select("user_id", "sku_id", "o_date", "o_sku_num")
                .toDF("o_user_id", "o_sku_id", "o_date", "o_sku_num")


        val action2order_df: DataFrame = action_df.join(order_new_df, action_df("user_id") === order_new_df("o_user_id") and action_df("a_date") === order_new_df("o_date"), "outer")

        println("action2order_df:" + action2order_df.count())
        val orderNotNull_df = action2order_df.filter($"o_user_id".isNotNull)
                .map { row =>
//                       case Row(user_id:Int, sku_id:Int ,a_date:Timestamp,a_num:Int,a_type:Int,o_user_id:Int,o_sku_id:Int,o_date:Timestamp,o_sku_num:Int)=>
                    val userID = row.getInt(5)
                    val date = row.getTimestamp(7)
                    val sku = row.getInt(6)
                    val aType = 0
                    val aNum = 0
                    val oNum = row.getInt(8)
                    val types=1
                    (userID, date, sku, aType, aNum, oNum,types)
                }
        val actionNotNull_df = action2order_df.filter($"user_id".isNotNull).filter($"o_user_id".isNull)
                .map { row =>
//                case Row(user_id:Int, sku_id:Int ,a_date:Timestamp,a_num:Int,a_type:Int,o_user_id:Int,o_sku_id:Int,o_date:Timestamp,o_sku_num:Int)=>
                    val userID = row.getInt(0)
                    val date = row.getTimestamp(2)
                    val sku = row.getInt(1)
                    val aType = row.getInt(4)
                    val aNum = row.getInt(3)
                    val oNum = 0
                    val types= 0
                    (userID, date, sku, aType, aNum, oNum,types)
                }

        val order2action_df = orderNotNull_df.union(actionNotNull_df).toDF("user_id", "date", "sku_id", "a_type", "a_num", "o_num","types")
//        println(order2action_df.count())
//        order2action_df.show()

        val all_df = order2action_df.join(user_id, "user_id")
                .join(sku_df, "sku_id")
                .select("user_id", "date", "sku_id", "a_type", "a_num", "o_num", "user_lv_cd", "cate","types")
//        println("all_df:" + all_df.count())
//        all_df.show()
        all_df
    }

    def featureProcess(df: DataFrame) = {
        val columns = Array("user_id", "date", "sku_id", "a_type", "a_num", "o_num", "user_lv_cd", "cate")
        //        val train_df = df.select("a_type", "a_num", "o_num", "user_lv_cd", "cate") //其中a_num,o_num是连续变量
        var stages = Array[PipelineStage]()
        val pipeline = new Pipeline()
        //归一化化
        val linearColumnsAssambel = new VectorAssembler().setInputCols(Array("o_num", "a_num")).setOutputCol("continuous_variable")
        val o_num_scaler = new StandardScaler()
                .setInputCol("continuous_variable").setOutputCol("continuous_variable" + "_scale")
        stages = stages :+ linearColumnsAssambel :+ o_num_scaler
        //独热编码
        val onehotColumns = Array("a_type", "user_lv_cd", "cate")
        for (column <- onehotColumns) {
            stages = stages ++ stageByOneHot(column)
        }
        //合并向量
        val vectorAssembler = new VectorAssembler()
                .setInputCols(onehotColumns.map(_ + "_onehot") :+ "continuous_variable_scale")
                .setOutputCol("features")
        stages = stages :+ vectorAssembler


        val model = pipeline.setStages(stages)
                .fit(df)
        val feature_df = model.transform(df).select("user_id", "date", "sku_id", "cate","types", "features")
        feature_df
    }

    def stageByOneHot(column: String): Array[PipelineStage] = {
        var stages = Array[PipelineStage]()
        val string2vector = new StringIndexer()
                .setInputCol(column)
                .setOutputCol(column + "_string2vector")
        stages = stages :+ string2vector
        val onehoter: OneHotEncoder = new OneHotEncoder()
                .setInputCol(column + "_string2vector").setOutputCol(column + "_onehot")
        stages = stages :+ onehoter
        stages
    }

    def kmeansTrian(df: DataFrame) = {
        var stages = Array[PipelineStage]()
        //定义模型
        val kmeans = new KMeans()
                .setFeaturesCol("features")
//        'random' and 'k-means||'
                .setInitMode("k-means||")
        stages = stages :+ kmeans
        val params:ParamMap = ParamMap(kmeans.k -> 30,
//            kmeans.tol->0.2,  //迭代算法收敛性
            kmeans.seed -> 1L,
            kmeans.maxIter -> 10
        )
        val pipeline = new Pipeline()
                .setStages(stages)
        val model=pipeline.fit(df,params)
        model.transform(df)
    }

    def kmeansTrianByParam(df: DataFrame,k:Int,maxIter:Int) = {
        println("########################################################################")
        println("k:"+k+"maxIter:"+maxIter)
        var stages = Array[PipelineStage]()
        //定义模型
        val kmeans = new KMeans()
                .setFeaturesCol("features")
//        'random' and 'k-means||'
                .setInitMode("k-means||")
                        .setK(k)
                        .setMaxIter(maxIter)
        stages = stages :+ kmeans
        val pipeline = new Pipeline()
                .setStages(stages)
        val model=pipeline.fit(df)
        model.transform(df)
    }
}

