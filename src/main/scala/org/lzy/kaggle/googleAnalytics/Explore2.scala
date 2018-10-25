package org.lzy.kaggle.googleAnalytics

import java.util.Date

import common.{SparkUtil, Utils}

object Explore2 {
  def main(args: Array[String]): Unit = {
    implicit val spark=SparkUtil.getSpark()
    val sc=spark.sparkContext
    sc.setLogLevel("warn")
    val util=new Utils(spark)
    val train=util.readToCSV(Constants.trainPath)
    train.show(false)
    val device_browser=train.select("device_browser").distinct()
    println(device_browser.count())
    device_browser.show(false)


    val device_operatingSystem=train.select("device_operatingSystem").distinct().sort()
    println(device_operatingSystem.count())
    device_operatingSystem.show(false)

    val geoNetwork_continent=train.select("geoNetwork_continent").distinct().sort()
    println(geoNetwork_continent.count())
    geoNetwork_continent.show(false)

    val geoNetwork_country=train.select("geoNetwork_country").distinct().sort()
    println(geoNetwork_country.count())
    geoNetwork_country.show(false)

    val geoNetwork_metro=train.select("geoNetwork_metro").distinct().sort()
    println(geoNetwork_metro.count())
    geoNetwork_metro.show(false)

    val geoNetwork_subContinent=train.select("geoNetwork_subContinent").distinct().sort()
    println(geoNetwork_subContinent.count())
    geoNetwork_subContinent.show(false)

    val trafficSource_campaign=train.select("trafficSource_campaign").distinct().sort()
    println(trafficSource_campaign.count())
    trafficSource_campaign.show(false)

    val trafficSource_medium=train.select("trafficSource_medium").distinct().sort()
    println(trafficSource_medium.count())
    trafficSource_medium.show(false)

    val trafficSource_source=train.select("trafficSource_source").distinct().sort()
    println(trafficSource_source.count())
    trafficSource_source.show(false)
  }
}
