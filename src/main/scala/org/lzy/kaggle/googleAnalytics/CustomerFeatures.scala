package org.lzy.kaggle.googleAnalytics

import com.salesforce.op.features.{FeatureBuilder, FeatureLike}
import com.salesforce.op.features.types._
import java.text.SimpleDateFormat
import java.util.Calendar

import com.salesforce.op.aggregators.SumRealNN
import org.joda.time.Duration

import scala.util.Try

/**
  * Auther: lzy
  * Description:
  * Date Created by： 9:26 on 2018/10/11
  * Modified By：
  */
trait CustomerFeatures extends Serializable {
  val sdf = new SimpleDateFormat("yyyyMMdd")

  def transfomNone(str: String) = {
    val newOp: Option[String] = str match {
      case "(not set)" => None
      case "(none)" => None
      case "not available in demo dataset" => None
      case _ => Some(str)
    }
    newOp
  }

  ////////////////////////////////////////////////////////////////////////////////
  //[特征工程]-基础特征
  /////////////////////////////////////////////////////////////////////////////////


  val channelGrouping = FeatureBuilder.PickList[Customer].extract(_.channelGrouping.toPickList).asPredictor
  //有空值，""也是字符串
  val date = FeatureBuilder.Date[Customer].extract(v => v.date.toDate).asPredictor
  val visitStartTime = FeatureBuilder.Date[Customer].extract(v => v.visitStartTime.map(_.toLong * 1000).toDate).asPredictor
  //    val date=FeatureBuilder.Text[Customer].extract(_.fullVisitorId.toText).asPredictor


  val fullVisitorId = FeatureBuilder.ID[Customer].extract(_.fullVisitorId.toID).asPredictor
  val sessionId = FeatureBuilder.Text[Customer].extract(_.fullVisitorId.toText).asPredictor
  val visitId = FeatureBuilder.Text[Customer].extract(_.fullVisitorId.toText).asPredictor
  val visitNumber = FeatureBuilder.RealNN[Customer].extract(_.visitNumber.getOrElse(0d).toRealNN).asPredictor

  val device_browser = FeatureBuilder.PickList[Customer].extract(v => transfomNone(v.device_browser).toPickList).asPredictor
  val device_deviceCategory = FeatureBuilder.PickList[Customer].extract(_.device_deviceCategory.toPickList).asPredictor
  val device_isMobile = FeatureBuilder.Binary[Customer].extract(_.device_isMobile.getOrElse(0d).toBinary).asPredictor
  val device_operatingSystem = FeatureBuilder.PickList[Customer].extract(v => transfomNone(v.device_operatingSystem).toPickList).asPredictor


  val geoNetwork_city = FeatureBuilder.City[Customer].extract(v => transfomNone(v.geoNetwork_city).toCity).asPredictor
  val geoNetwork_continent = FeatureBuilder.PickList[Customer].extract(v => transfomNone(v.geoNetwork_continent).toPickList).asPredictor
  val geoNetwork_country = FeatureBuilder.Country[Customer].extract(_.geoNetwork_country.toCountry).asPredictor
  val geoNetwork_metro = FeatureBuilder.PickList[Customer].extract(v => transfomNone(v.geoNetwork_metro).toPickList).asPredictor
  val geoNetwork_networkDomain = FeatureBuilder.URL[Customer].extract(_.geoNetwork_networkDomain.toURL).asPredictor
  val geoNetwork_region = FeatureBuilder.PickList[Customer].extract(v => transfomNone(v.geoNetwork_region).toPickList).asPredictor
  val geoNetwork_subContinent = FeatureBuilder.PickList[Customer].extract(_.geoNetwork_subContinent.toPickList).asPredictor

  val totals_bounces = FeatureBuilder.Real[Customer].extract(_.totals_bounces.toReal).asPredictor
  val totals_hits = FeatureBuilder.Real[Customer].extract(_.totals_hits.toReal).asPredictor
  val totals_newVisits = FeatureBuilder.Real[Customer].extract(_.totals_newVisits.toReal).asPredictor
  val totals_pageviews = FeatureBuilder.Real[Customer].extract(_.totals_pageviews.toReal).asPredictor


  val trafficSource_adContent = FeatureBuilder.PickList[Customer].extract(_.trafficSource_adContent.toPickList).asPredictor
  val trafficSource_campaign = FeatureBuilder.PickList[Customer].extract(v => transfomNone(v.trafficSource_campaign).toPickList).asPredictor
  val trafficSource_isTrueDirect = FeatureBuilder.Binary[Customer].extract(_.trafficSource_isTrueDirect.getOrElse(0d).toBinary).asPredictor
  val trafficSource_keyword = FeatureBuilder.Text[Customer].extract(_.geoNetwork_subContinent.toText).asPredictor
  val trafficSource_medium = FeatureBuilder.PickList[Customer].extract(v => transfomNone(v.trafficSource_medium).toPickList).asPredictor
  val trafficSource_referralPath = FeatureBuilder.PickList[Customer].extract(_.trafficSource_referralPath.toPickList).asPredictor
  val trafficSource_source = FeatureBuilder.PickList[Customer].extract(_.trafficSource_source.toPickList).asPredictor

  val totals_transactionRevenue = FeatureBuilder.RealNN[Customer].extract(x => math.log1p(x.totals_transactionRevenue.getOrElse(0d)).toRealNN).asResponse


  ////////////////////////////////////////////////////////////////////////////////
  //[特征工程]-统计特征
  /////////////////////////////////////////////////////////////////////////////////
  //是否visitID和访问时间相同
  val isVisitIdEqStartTime = FeatureBuilder.Binary[Customer].extract(v => {
    val iss = if (v.visitId == v.visitStartTime.getOrElse(0d).toString) 1d else 0d
      iss.toBinary
  }).asPredictor
  //获取星期几
  val week=FeatureBuilder.PickList[Customer].extract(v=>{
    val cal= Calendar.getInstance()
    cal.setTimeInMillis(v.visitStartTime.getOrElse(0d).toLong)
    cal.get(Calendar.DAY_OF_WEEK).toString.toPickList
  }).asPredictor

val source_country=FeatureBuilder.PickList[Customer].extract(v=>{
  val source_country=v.trafficSource_source+"_"+v.geoNetwork_country
  source_country.toPickList
}).asPredictor
  val campaign_medium=FeatureBuilder.PickList[Customer].extract(v=>{
    val campaign_medium=v.trafficSource_campaign+"_"+v.trafficSource_medium
    campaign_medium.toPickList
  }).asPredictor
  val browser_category=FeatureBuilder.PickList[Customer].extract(v=>{
    val browser_category=v.device_browser+"_"+v.device_deviceCategory
    browser_category.toPickList
  }).asPredictor
  val browser_os=FeatureBuilder.PickList[Customer].extract(v=>{
    val browser_os=v.device_browser+"_"+v.device_operatingSystem
    browser_os.toPickList
  }).asPredictor

  val device_deviceCategory_cahnnelGrouping=FeatureBuilder.PickList[Customer].extract(v=>{
    val device_deviceCategory_cahnnelGrouping=v.device_deviceCategory+"_"+v.channelGrouping
    device_deviceCategory_cahnnelGrouping.toPickList
  }).asPredictor

    val channelGrouping_browser=FeatureBuilder.PickList[Customer].extract(v=>{
    val channelGrouping_browser=v.device_browser+"_"+v.channelGrouping
      channelGrouping_browser.toPickList
  }).asPredictor


      val channelGrouping_os=FeatureBuilder.PickList[Customer].extract(v=>{
      val channelGrouping_os=v.channelGrouping+"_"+v.device_operatingSystem
        channelGrouping_os.toPickList
    }).asPredictor



//        val allVisitNum=FeatureBuilder.RealNN[Customer].extract(_.visitNumber.getOrElse(0d).toRealNN)
//      .aggregate(SumRealNN).window(Duration.standardDays(7))
//      .asPredictor
////////////////////////////////////////////////////////////////////////////////
//[特征工程]-最终特征选择
/////////////////////////////////////////////////////////////////////////////////
val customerFeatures: FeatureLike[OPVector] = Seq (channelGrouping, date, visitNumber, visitStartTime,
device_browser, device_deviceCategory, device_isMobile, device_operatingSystem,
geoNetwork_city, geoNetwork_continent, geoNetwork_country, geoNetwork_metro, geoNetwork_networkDomain, geoNetwork_region, geoNetwork_subContinent,
totals_bounces, totals_hits, totals_newVisits, totals_pageviews,
trafficSource_adContent, trafficSource_campaign, trafficSource_isTrueDirect, trafficSource_medium, trafficSource_referralPath, trafficSource_source
  ,
  isVisitIdEqStartTime,week,source_country,campaign_medium,browser_category,browser_os,device_deviceCategory_cahnnelGrouping,
  channelGrouping_browser,channelGrouping_os

)
.transmogrify ()

////////////////////////////////////////////////////////////////////////////////
//[特征工程]-数据泄露及空值处理
/////////////////////////////////////////////////////////////////////////////////
val sanityCheck = true
val finalFeatures = if (sanityCheck) totals_transactionRevenue.sanityCheck (customerFeatures) else customerFeatures
}
