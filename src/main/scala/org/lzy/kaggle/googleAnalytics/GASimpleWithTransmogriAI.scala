package scala.org.lzy.kaggle.googleAnalytics

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
    //    val channelGrouping=FeatureBuilder.
    //    val date=FeatureBuilder.
    //    val fullVisitorId=FeatureBuilder.
    //    val sessionId=FeatureBuilder.
    //    val visitId=FeatureBuilder.
    val visitNumber = FeatureBuilder.RealNN[Customer].extract(_.visitNumber.toRealNN).asPredictor
    val visitStartTime = FeatureBuilder.DateTime[Customer].extract(v => (v.visitStartTime * 1000).toDateTime).asPredictor
    //    val device_browser=FeatureBuilder.
    //    val device_deviceCategory=FeatureBuilder.
    val device_isMobile = FeatureBuilder.Binary[Customer].extract(_.device_isMobile.toBinary).asPredictor
    //    val device_operatingSystem=FeatureBuilder.
    //    val geoNetwork_city=FeatureBuilder.
    //    val geoNetwork_continent=FeatureBuilder.
    //    val geoNetwork_country=FeatureBuilder.
    //    val geoNetwork_metro=FeatureBuilder.
    //    val geoNetwork_networkDomain=FeatureBuilder.
    //    val geoNetwork_region=FeatureBuilder.
    //    val geoNetwork_subContinent=FeatureBuilder.
    val totals_bounces = FeatureBuilder.Real[Customer].extract(_.totals_bounces.toReal).asPredictor
    val totals_hits = FeatureBuilder.RealNN[Customer].extract(_.totals_hits.toRealNN).asPredictor
    val totals_newVisits = FeatureBuilder.Real[Customer].extract(_.totals_newVisits.toReal).asPredictor
    val totals_pageviews = FeatureBuilder.Real[Customer].extract(_.totals_pageviews.toReal).asPredictor
    val totals_transactionRevenue = FeatureBuilder.RealNN[Customer].extract(_.totals_transactionRevenue.getOrElse(0d).toRealNN).asResponse
    //    val trafficSource_adContent=FeatureBuilder.
    //    val trafficSource_campaign=FeatureBuilder.
    val trafficSource_isTrueDirect = FeatureBuilder.Binary[Customer].extract(_.trafficSource_isTrueDirect.getOrElse(0d).toBinary).asPredictor
    //    val trafficSource_keyword=FeatureBuilder.
    //    val trafficSource_medium=FeatureBuilder.
    //    val trafficSource_referralPath=FeatureBuilder.
    //    val trafficSource_source=FeatureBuilder.
    //    val =FeatureBuilder.=FeatureBuilder.


  }
}
