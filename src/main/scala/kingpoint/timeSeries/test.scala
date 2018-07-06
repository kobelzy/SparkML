package kingpoint.timeSeries

import org.apache.spark.sql.SparkSession

/**
  * Auther: lzy
  * Description:
  * Date Created by： 15:28 on 2018/6/27
  * Modified By：
  */

class test {
    def main(args: Array[String]): Unit = {
        val spark=SparkSession.builder().appName("test").master("local[*]").getOrCreate()
        val train=spark.read.csv("hdfs://master:9000/user/hbn/train.csv")
        train.show(false)
    }
}
