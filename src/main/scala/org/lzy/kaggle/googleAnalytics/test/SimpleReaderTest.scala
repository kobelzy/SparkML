package org.lzy.kaggle.googleAnalytics.test

import java.text.SimpleDateFormat

import com.salesforce.op.readers.DataReaders
import common.{SparkUtil, Utils}
import org.apache.spark.sql.Row
import org.lzy.kaggle.googleAnalytics.{Constants, Customer}
import org.lzy.transmogriAI.Passenger

import scala.util.Try

object SimpleReaderTest {


  def main(args: Array[String]): Unit = {
    run1()
  }

  def run1() = {
    implicit val spark = SparkUtil.getSpark()
    import spark.implicits._
    val util=new Utils(spark)
//    val train=util.readToCSV(Constants.trainPath)
//    train.show(false)


    val trainDataReader = DataReaders.Simple.csvCase[Customer](
      path = Option(Constants.trainPath))
    val train_DS = trainDataReader.readDataset()
    println("---")
    train_DS.show(false)

    val csvFilePath = ClassLoader.getSystemResource("TransmogrifData/TitanicPassengersTrainData.csv").toString
    val passengerReader = DataReaders.Simple.csvCase[Passenger](
      path = Option(csvFilePath),
      key = _.id.toString
    )
    passengerReader.readDataset().show(false)
  }


  def run2()={
    implicit val spark = SparkUtil.getSpark()
    import spark.implicits._

    val sdf = new SimpleDateFormat("yyyyMMdd")

    val train = spark.read.option("header", "true")
      .option("nullValue", "NA")
//      .option("inferSchema", "true")
//      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
      .csv(Constants.trainPath)
//        .map{case Row(channelGrouping_,date_,fullVisitorId_,sessionId_,visitId_,visitNumber_,visitStartTime_,device_browser_,device_deviceCategory_,device_isMobile_,device_operatingSystem_,geoNetwork_city_,geoNetwork_continent_,geoNetwork_country_,geoNetwork_metro_,geoNetwork_networkDomain_,geoNetwork_region_,geoNetwork_subContinent_,totals_bounces_,totals_hits_,totals_newVisits_,totals_pageviews_,totals_transactionRevenue_,trafficSource_adContent_,trafficSource_campaign_,trafficSource_isTrueDirect_,trafficSource_keyword_,trafficSource_medium_,trafficSource_referralPath_,trafficSource_source)=>
//          val channelGrouping=channelGrouping_.toString
//          val date:Any=Try(sdf.parse(date_.toString).getTime).getOrElse(None)
//
//        }

    train.show(false)
  }
}
