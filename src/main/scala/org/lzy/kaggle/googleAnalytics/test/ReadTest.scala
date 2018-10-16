package org.lzy.kaggle.googleAnalytics.test

import java.util.Date

import com.salesforce.op.readers.{CSVProductReader, DataReaders}
import common.SparkUtil
import org.lzy.kaggle.googleAnalytics.{Constants, Customer2}

object ReadTest {
  def main(args: Array[String]): Unit = {
//    implicit val spark=SparkUtil.getSpark()
//    import spark.implicits._
//    val testDataReader: CSVProductReader[Customer2] = DataReaders.Simple.csvCase[Customer2](path = Option(Constants.testPath), key = v => v.fullVisitorId + "")
//    testDataReader.readDataset().show(false)
    val date=new Date()
    date.setTime(52337736688L*1000)
    println(date)

  }
}
