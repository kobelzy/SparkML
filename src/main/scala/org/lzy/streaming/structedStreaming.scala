package org.lzy.streaming

import common.SparkUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}


object structedStreaming {
  def run() = {
    val sparkSession=SparkUtil.getSpark()
    val df:DataFrame=sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic.*")
      .load()

    val jdbcWriter:JDBCSink=new JDBCSink("url","user","pwd")

  val query=  df.writeStream
      .foreach(jdbcWriter)
      .outputMode(OutputMode.Update())
      .trigger(ProcessingTime("25 seconds"))
      .start()

    query.awaitTermination()
  }
}
