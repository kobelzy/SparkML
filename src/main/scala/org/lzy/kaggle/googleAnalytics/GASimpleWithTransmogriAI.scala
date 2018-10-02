package scala.org.lzy.kaggle.googleAnalytics

import java.text.SimpleDateFormat

import com.salesforce.op.features.FeatureBuilder
import com.salesforce.op.features.types._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.lzy.kaggle.googleAnalytics.CaseClass.Customer

/**
  * Created by Administrator on 2018/10/1.
  */
object GASimpleWithTransmogriAI {
  val basePath = "E:/Dataset/GoogleAnalytics/"
  val sdf = new SimpleDateFormat("yyyyMMdd")
  sdf.parse("").getTime

  def run() = {
    val trainPath = basePath + "source/extracted_fields_test.csv"
    val testPath = basePath + "source/extracted_fields_test.csv"

    // Set up a SparkSession as normal
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")

    implicit val spark = SparkSession.builder.config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    /*
    基础数据
     */
    val channelGrouping = FeatureBuilder.PickList[Customer].extract(_.channelGrouping.toPickList).asPredictor
    val date = FeatureBuilder.Date[Customer].extract(v => sdf.parse(v.date).getTime.toDate).asPredictor

    val fullVisitorId = FeatureBuilder.ID[Customer].extract(_.fullVisitorId.toID).asPredictor
    val sessionId = FeatureBuilder.ID[Customer].extract(_.fullVisitorId.toID).asPredictor
    val visitId = FeatureBuilder.ID[Customer].extract(_.fullVisitorId.toID).asPredictor

    val visitNumber = FeatureBuilder.RealNN[Customer].extract(_.visitNumber.toRealNN).asPredictor
    val visitStartTime = FeatureBuilder.DateTime[Customer].extract(v => (v.visitStartTime * 1000).toDateTime).asPredictor


    val device_browser = FeatureBuilder.PickList[Customer].extract(_.device_browser.toPickList).asPredictor
    val device_deviceCategory = FeatureBuilder.PickList[Customer].extract(_.device_deviceCategory.toPickList).asPredictor
    val device_isMobile = FeatureBuilder.Binary[Customer].extract(_.device_isMobile.toBinary).asPredictor
    val device_operatingSystem = FeatureBuilder.PickList[Customer].extract(_.device_operatingSystem.toPickList).asPredictor


    val geoNetwork_city = FeatureBuilder.City[Customer].extract(_.geoNetwork_city.toCity).asPredictor
    val geoNetwork_continent = FeatureBuilder.PickList[Customer].extract(_.geoNetwork_continent.toPickList).asPredictor
    val geoNetwork_country = FeatureBuilder.Country[Customer].extract(_.geoNetwork_country.toCountry).asPredictor
    val geoNetwork_metro = FeatureBuilder.PickList[Customer].extract(_.geoNetwork_metro.toPickList).asPredictor
    val geoNetwork_networkDomain = FeatureBuilder.URL[Customer].extract(_.geoNetwork_networkDomain.toURL).asPredictor
    val geoNetwork_region = FeatureBuilder.PickList[Customer].extract(_.geoNetwork_region.toPickList).asPredictor
    val geoNetwork_subContinent = FeatureBuilder.PickList[Customer].extract(_.geoNetwork_subContinent.toPickList).asPredictor


    val totals_bounces = FeatureBuilder.Real[Customer].extract(_.totals_bounces.toReal).asPredictor
    val totals_hits = FeatureBuilder.RealNN[Customer].extract(_.totals_hits.toRealNN).asPredictor
    val totals_newVisits = FeatureBuilder.Real[Customer].extract(_.totals_newVisits.toReal).asPredictor
    val totals_pageviews = FeatureBuilder.Real[Customer].extract(_.totals_pageviews.toReal).asPredictor
    val totals_transactionRevenue = FeatureBuilder.RealNN[Customer].extract(_.totals_transactionRevenue.getOrElse(0d).toRealNN).asResponse


    val trafficSource_adContent = FeatureBuilder.PickList[Customer].extract(_.trafficSource_adContent.toPickList).asPredictor
    val trafficSource_campaign = FeatureBuilder.PickList[Customer].extract(_.trafficSource_campaign.toPickList).asPredictor
    val trafficSource_isTrueDirect = FeatureBuilder.Binary[Customer].extract(_.trafficSource_isTrueDirect.getOrElse(0d).toBinary).asPredictor
    val trafficSource_keyword = FeatureBuilder.Text[Customer].extract(_.geoNetwork_subContinent.toText).asPredictor
    val trafficSource_medium = FeatureBuilder.PickList[Customer].extract(_.trafficSource_medium.toPickList).asPredictor
    val trafficSource_referralPath = FeatureBuilder.PickList[Customer].extract(_.trafficSource_referralPath.toPickList).asPredictor
    val trafficSource_source = FeatureBuilder.PickList[Customer].extract(_.trafficSource_source.toPickList).asPredictor


  }
}
