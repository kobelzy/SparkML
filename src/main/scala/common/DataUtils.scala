package common

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
/**
  * Auther: lzy
  * Description:
  * Date Created by： 17:04 on 2018/6/27
  * Modified By：
  */
class DataUtils(spark: SparkSession) {



    def readToCSV(path: String,sep:String=","): DataFrame = {
        val data = spark.read.option("header", "true")
          .option("nullValue", "NA")
          .option("inferSchema", "true")
          .option("sep",sep)
          .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
          .csv(path)
        data
    }
/***
 * 功能实现:将df持久化为csv到指定路径中
 *
 * Author: Lzy
 * Date: 2018/6/27 17:15
 * Param: [df,path]
 * Return: void
 */
    def writeToCSV(df:DataFrame,path:String)={
        df.coalesce(1).write.option("header", "true")
                .mode(SaveMode.Overwrite)
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                //          .option("nullValue", "NA")
                .csv(path)

    }
}
