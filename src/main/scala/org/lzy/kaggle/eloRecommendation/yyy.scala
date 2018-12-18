package org.lzy.kaggle.eloRecommendation

import common.{DataUtils, SparkUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Auther: lzy
  * Description:
  * Date Created by： 18:57 on 2018/12/18
  * Modified By：
  */

object yyy {
    def main(args: Array[String]): Unit = {
        val spark=SparkUtil.getSpark()
        val sc=spark.sparkContext
        import spark.implicits._
        val map=sc.textFile(EloConstants.basePath+"source/aa.csv")
                .filter(!_.contains("card_id"))
                .map(line=>{
                    val splites=line.split(",")
                    (splites(0),splites(1))
                }).collectAsMap()
        case class what(card_id:String,target:String)
         val result=sc.textFile(EloConstants.basePath+"source/test.csv")
                 .filter(!_.contains("card_id"))
                 .map(line=>{
                     val card_id=line.split(",")(1)
                     val target=map.getOrElse(card_id,"0.0")
                     (card_id,target)
                 }).toDF("card_id","target")
        val util=new DataUtils(spark )
        util.to_csv(result,EloConstants.basePath+"result/submit.csv")

    }
}
