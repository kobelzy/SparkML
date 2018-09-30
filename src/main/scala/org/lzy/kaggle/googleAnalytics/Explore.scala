package org.lzy.kaggle.googleAnalytics

import common.Utils
import org.apache.spark.sql.SparkSession

import scala.util.Try

object Explore {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("explore").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val utils = new Utils(spark)
    //    val train_df=utils.readToCSV("D:\\Dataset\\GoogleAnalytics\\source\\train.csv","(,[^ ])")
    //    val teset_df=utils.readToCSV("D:\\Dataset\\GoogleAnalytics\\source\\test.csv")
    //    train_df.printSchema()
    //    train_df.show(false)
    val train_rdd = sc.textFile("E:\\Dataset\\GoogleAnalytics\\source\\train.csv")
/*    train_rdd
//      .take(10000)
      .filter(!_.contains("criteriaParameters\"\": \"\"not available in demo dataset"))
//      .filter(!_.contains("channelGrouping"))
      .map(line=>line.split("(,[^ ])").mkString("||")).foreach(println)*/
    train_rdd.filter(!_.contains("channelGrouping"))
      .map(line=>{line.split("(,[^ ])")(8)
        .split(",",-1)(4)}).foreach(println)


    //Organic Search|0160902|{""browser"": ""Internet Explorer"", ""browserVersion"": ""not available in demo dataset"", ""browserSize"": ""not available in demo dataset"", ""operatingSystem"": ""Windows"", ""operatingSystemVersion"": ""not available in demo dataset"", ""isMobile"": false, ""mobileDeviceBranding"": ""not available in demo dataset"", ""mobileDeviceModel"": ""not available in demo dataset"", ""mobileInputSelector"": ""not available in demo dataset"", ""mobileDeviceInfo"": ""not available in demo dataset"", ""mobileDeviceMarketingName"": ""not available in demo dataset"", ""flashVersion"": ""not available in demo dataset"", ""language"": ""not available in demo dataset"", ""screenColors"": ""not available in demo dataset"", ""screenResolution"": ""not available in demo dataset"", ""deviceCategory"": ""desktop""}"
      // |445454811831400414|{""continent"": ""Europe"", ""subContinent"": ""Western Europe"", ""country"": ""Austria"", ""region"": ""not available in demo dataset"", ""metro"": ""not available in demo dataset"", ""city"": ""not available in demo dataset"", ""cityId"": ""not available in demo dataset"", ""networkDomain"": ""spar.at"", ""latitude"": ""not available in demo dataset"", ""longitude"": ""not available in demo dataset"", ""networkLocation"": ""not available in demo dataset""}"
      //
      // |445454811831400414_1472805784|ot Socially Engaged|{""visits"": ""1"", ""hits"": ""1"", ""pageviews"": ""1"", ""bounces"": ""1"", ""newVisits"": ""1""}"|{""campaign"": ""(not set)"", ""source"": ""google"", ""medium"": ""organic"", ""keyword"": ""(not provided)"", ""adwordsClickInfo"": {""criteriaParameters"": ""not available in demo dataset""}}"|472805784||472805784
      val train=train_rdd.take(10)
          .filter(!_.contains("channelGrouping"))
        .map(line => {
      val splits = line.replace("\"","").replace("{","").replace("}","").split("(,[^ ])")
      val channelGropuing = splits(0)
      val date = splits(1)


      val device = splits(2).split(",")
      val browser = device(0).split(":").last
      val broserVersion = device(1).split(":").last
      val broserSize = device(2).split(":").last
      val operatingSystem = device(3).split(":").last
      val operatingSystemVersion = device(4).split(":").last
      val isMobile = device(5).split(":").last
      val mobileDeviceBranding = device(6).split(":").last
      val mobileDeviceModel = device(7).split(":").last
      val mobileInputSelector = device(8).split(":").last
      val mobileDeviceInfo = device(9).split(":").last
      val mobileDeviceMarketingName = device(10).split(":").last
      val flashVersion = device(11).split(":").last
      val language = device(12).split(":").last
      val screenColors = device(13).split(":").last
      val screenResolution = device(14).split(":").last
      val deviceCategory = device(15).split(":").last


      val fullVisiorId = splits(3)

      val geoNetwork = splits(4).split(",")
      val continent = geoNetwork(0).split(":").last
      val subContinent = geoNetwork(1).split(":").last
      val country = geoNetwork(2).split(":").last
      val region = geoNetwork(3).split(":").last
      val metro = geoNetwork(4).split(":").last
      val city = geoNetwork(5).split(":").last
      val cityId = geoNetwork(6).split(":").last
      val networkDomain = geoNetwork(7).split(":").last
      val latitude = geoNetwork(8).split(":").last
      val longitude = geoNetwork(9).split(":").last
      val networkLocation = geoNetwork(10).split(":").last

      val sessionId = splits(5)

      val socialEngagementType = splits(6)

      //	{"visits": "1", "hits": "4", "pageviews": "4"}  测试机
      //  {"visits": "1", "hits": "5", "pageviews": "5", "newVisits": "1"}  训练集
      val totals = splits(7).split(",")
      val visits = totals(0).split(":").last
      val hists = totals(1).split(":").last
      val pageviews = totals(2).split(":").last
      val newVisits = totals(3).split(":").last
//      {""campaign"": ""(not set)"", ""source"": ""google"",
// ""medium"": ""organic"", ""keyword"": ""(not provided)"",
// ""adwordsClickInfo"": {""criteriaParameters"": ""not available in demo dataset""}}
      val trafficSource = splits(8).split(",")
      val campaign = trafficSource(0).split(":").last
      val source = trafficSource(1).split(":").last
      val medium = trafficSource(2).split(":").last
      val keyword = trafficSource(3).split(":").last
      val adwordsClickInfo = trafficSource(4).split(":").last
//      val isTrueDirect = trafficSource(5).split(":").last

      val visitId = splits(9)

      val visitNumber = tInt(splits(10))
      val visitStartTime = tLong(splits(11))

      Array(channelGropuing,date,browser,broserVersion,broserSize,operatingSystem,operatingSystemVersion,isMobile,mobileDeviceBranding,mobileDeviceModel,mobileInputSelector,mobileDeviceInfo,mobileDeviceMarketingName,flashVersion,language,screenColors,screenResolution,deviceCategory,fullVisiorId,continent,subContinent,country,region,metro,city,cityId,networkDomain,latitude,longitude,networkLocation,sessionId,socialEngagementType,visits,hists,pageviews,newVisits,campaign,source,medium,keyword,adwordsClickInfo,
//        isTrueDirect,
        visitId,visitNumber,visitStartTime).mkString("|")
    })

//    train.foreach(println)
  }

 def tLong(value:String)=Try(value.toLong).getOrElse(-9999)
 def tInt(value:String)=Try(value.toInt).getOrElse(-9999)
}
