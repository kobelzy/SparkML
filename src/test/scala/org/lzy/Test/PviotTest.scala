package org.lzy.Test

import common.SparkUtil
import org.apache.spark.sql.functions._

object PviotTest {
  val spark = SparkUtil.getSpark()
  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    run
  }

  def run = {
    val df = spark.createDataFrame(Seq(("2018-01", "项目1", 100), ("2018-01", "项目2", 200), ("2018-01", "项目3", 300),
      ("2018-02", "项目1", 1000), ("2018-02", "项目2", 2000), ("2018-03", "项目x", 999)
    )).toDF("年月", "项目", "收入")
    df.show(false)
    val dfPivot = df.groupBy("年月")
      .pivot("项目", Seq("项目1", "项目2", "项目3", "项目4"))
      .agg(sum("收入"))
    dfPivot.show(false)

    df.groupBy("年月")
      .pivot("项目")
      .agg(sum("收入"))
      .show(false)

    df.groupBy("年月")
      .pivot("项目", Seq("项目1", "项目2"))
      .agg(sum("收入"))
      .show(false)
    val map=Map(1->"a")
  }
}
