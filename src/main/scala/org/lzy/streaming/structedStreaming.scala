package org.lzy.streaming

import common.SparkUtil
import org.apache.spark.sql.{DataFrame, ForeachWriter}
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}


object structedStreaming {
  def writer={
    private var testUtils: KafkaTestUtils = _
  }
  def run() = {
    val sparkSession=SparkUtil.getSpark()
    import sparkSession.implicits._
    val df:DataFrame=sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic.*")
      .load()

    val jdbcWriter=new JDBCSink("url","user","pwd")

  val query=  df.writeStream
      .foreach(jdbcWriter)
      .outputMode(OutputMode.Update())
      .trigger(ProcessingTime("25 seconds"))
      .start()

    query.awaitTermination()
  }
}
