package org.lzy.kaggle.googleAnalytics

import com.salesforce.op.features.{FeatureBuilder, FeatureLike}
import com.salesforce.op.features.types._
import java.text.SimpleDateFormat
import scala.util.Try
/**
  * Auther: lzy
  * Description:
  * Date Created by： 9:26 on 2018/10/11
  * Modified By：
  */
trait CustomerFeatures extends Serializable{
    val sdf = new SimpleDateFormat("yyyyMMdd")
    ////////////////////////////////////////////////////////////////////////////////
    //[特征工程]-基础特征
    /////////////////////////////////////////////////////////////////////////////////




    val channelGrouping = FeatureBuilder.PickList[Customer].extract(_.channelGrouping.toPickList).asPredictor
    //有空值，""也是字符串
    val date = FeatureBuilder.Date[Customer].extract(v => v.date.toDate).asPredictor
    val visitStartTime = FeatureBuilder.DateTime[Customer].extract(v => v.visitStartTime.map(_.toLong * 1000).toDateTime).asPredictor
//    val date=FeatureBuilder.Text[Customer].extract(_.fullVisitorId.toText).asPredictor


    val fullVisitorId = FeatureBuilder.ID[Customer].extract(_.fullVisitorId.toID).asPredictor
    val sessionId = FeatureBuilder.Text[Customer].extract(_.fullVisitorId.toText).asPredictor
    val visitId = FeatureBuilder.Text[Customer].extract(_.fullVisitorId.toText).asPredictor
    val visitNumber = FeatureBuilder.RealNN[Customer].extract(_.visitNumber.getOrElse(0d).toRealNN).asPredictor

    val device_browser = FeatureBuilder.PickList[Customer].extract(_.device_browser.toPickList).asPredictor
    val device_deviceCategory = FeatureBuilder.PickList[Customer].extract(_.device_deviceCategory.toPickList).asPredictor
    val device_isMobile = FeatureBuilder.Binary[Customer].extract(_.device_isMobile.getOrElse(0d).toBinary).asPredictor
    val device_operatingSystem = FeatureBuilder.PickList[Customer].extract(_.device_operatingSystem.toPickList).asPredictor
    val geoNetwork_city = FeatureBuilder.City[Customer].extract(_.geoNetwork_city.toCity).asPredictor
    val geoNetwork_continent = FeatureBuilder.PickList[Customer].extract(_.geoNetwork_continent.toPickList).asPredictor
    val geoNetwork_country = FeatureBuilder.Country[Customer].extract(_.geoNetwork_country.toCountry).asPredictor
    val geoNetwork_metro = FeatureBuilder.PickList[Customer].extract(_.geoNetwork_metro.toPickList).asPredictor
    val geoNetwork_networkDomain = FeatureBuilder.URL[Customer].extract(_.geoNetwork_networkDomain.toURL).asPredictor
    val geoNetwork_region = FeatureBuilder.PickList[Customer].extract(_.geoNetwork_region.toPickList).asPredictor
    val geoNetwork_subContinent = FeatureBuilder.PickList[Customer].extract(_.geoNetwork_subContinent.toPickList).asPredictor
    val totals_bounces = FeatureBuilder.Real[Customer].extract(_.totals_bounces.toReal).asPredictor
    val totals_hits = FeatureBuilder.Real[Customer].extract(_.totals_hits.toReal).asPredictor
    val totals_newVisits = FeatureBuilder.Real[Customer].extract(_.totals_newVisits.toReal).asPredictor
    val totals_pageviews = FeatureBuilder.Real[Customer].extract(_.totals_pageviews.toReal).asPredictor


    val trafficSource_adContent = FeatureBuilder.PickList[Customer].extract(_.trafficSource_adContent.toPickList).asPredictor
    val trafficSource_campaign = FeatureBuilder.PickList[Customer].extract(_.trafficSource_campaign.toPickList).asPredictor
    val trafficSource_isTrueDirect = FeatureBuilder.Binary[Customer].extract(_.trafficSource_isTrueDirect.getOrElse(0d).toBinary).asPredictor
    val trafficSource_keyword = FeatureBuilder.Text[Customer].extract(_.geoNetwork_subContinent.toText).asPredictor
    val trafficSource_medium = FeatureBuilder.PickList[Customer].extract(_.trafficSource_medium.toPickList).asPredictor
    val trafficSource_referralPath = FeatureBuilder.PickList[Customer].extract(_.trafficSource_referralPath.toPickList).asPredictor
    val trafficSource_source = FeatureBuilder.PickList[Customer].extract(_.trafficSource_source.toPickList).asPredictor

    val totals_transactionRevenue = FeatureBuilder.RealNN[Customer].extract(x=>math.log1p(x.totals_transactionRevenue.getOrElse(0d)).toRealNN).asResponse


    ////////////////////////////////////////////////////////////////////////////////
    //[特征工程]-统计特征
    /////////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////////
    //[特征工程]-最终特征选择
    /////////////////////////////////////////////////////////////////////////////////
    val customerFeatures: FeatureLike[OPVector] = Seq(channelGrouping, date,  visitNumber, visitStartTime, device_browser, device_deviceCategory, device_isMobile, device_operatingSystem, geoNetwork_city, geoNetwork_continent, geoNetwork_country, geoNetwork_metro, geoNetwork_networkDomain, geoNetwork_region, geoNetwork_subContinent, totals_bounces, totals_hits, totals_newVisits, totals_pageviews, trafficSource_adContent, trafficSource_campaign, trafficSource_isTrueDirect, trafficSource_keyword, trafficSource_medium, trafficSource_referralPath, trafficSource_source)
            .transmogrify()

    ////////////////////////////////////////////////////////////////////////////////
    //[特征工程]-数据泄露及空值处理
    /////////////////////////////////////////////////////////////////////////////////
    val sanityCheck = true
    val finalFeatures = if (sanityCheck) totals_transactionRevenue.sanityCheck(customerFeatures) else customerFeatures
}
