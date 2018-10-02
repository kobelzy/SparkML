package scala.org.lzy.kaggle.googleAnalytics

import java.text.SimpleDateFormat

import com.salesforce.op.{OpWorkflow, _}
import com.salesforce.op.evaluators.Evaluators
import com.salesforce.op.features.types._
import com.salesforce.op.features.{FeatureBuilder, FeatureLike}
import com.salesforce.op.readers.DataReaders
import com.salesforce.op.stages.impl.regression.RegressionModelSelector
import com.salesforce.op.stages.impl.regression.RegressionModelsToTry._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.lzy.kaggle.googleAnalytics.CaseClass.Customer

/**
  * Created by Administrator on 2018/10/1.
  */
object GASimpleWithTransmogriAI {
  val basePath = "E:/Dataset/GoogleAnalytics/"
  val sdf = new SimpleDateFormat("yyyyMMdd")

  def main(args: Array[String]): Unit = {


    run()
  }
  def run() = {
    val trainPath = basePath + "source/extracted_fields_test.csv"
    val testPath = basePath + "source/extracted_fields_test.csv"
    // Set up a SparkSession as normal
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")

    implicit val spark = SparkSession.builder.config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    ////////////////////////////////////////////////////////////////////////////////
    //基础特征
    /////////////////////////////////////////////////////////////////////////////////
    val channelGrouping = FeatureBuilder.PickList[Customer].extract(_.channelGrouping.toPickList).asPredictor
    val date = FeatureBuilder.Date[Customer].extract(v => sdf.parse(v.date).getTime.toDate).asPredictor

    val fullVisitorId = FeatureBuilder.ID[Customer].extract(_.fullVisitorId.toID).asPredictor
    val sessionId = FeatureBuilder.ID[Customer].extract(_.fullVisitorId.toID).asPredictor
    val visitId = FeatureBuilder.ID[Customer].extract(_.fullVisitorId.toID).asPredictor

    val visitNumber = FeatureBuilder.RealNN[Customer].extract(_.visitNumber.getOrElse(0d).toRealNN).asPredictor
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

    ////////////////////////////////////////////////////////////////////////////////
    //统计特征
    /////////////////////////////////////////////////////////////////////////////////


    ////////////////////////////////////////////////////////////////////////////////
    //创建特征向量
    /////////////////////////////////////////////////////////////////////////////////
    val customerFeatures = Seq(channelGrouping, date, fullVisitorId, sessionId, visitId, visitNumber, visitStartTime, device_browser, device_deviceCategory, device_isMobile, device_operatingSystem, geoNetwork_city, geoNetwork_continent, geoNetwork_country, geoNetwork_metro, geoNetwork_networkDomain, geoNetwork_region, geoNetwork_subContinent, totals_bounces, totals_hits, totals_newVisits, totals_pageviews, trafficSource_adContent, trafficSource_campaign, trafficSource_isTrueDirect, trafficSource_keyword, trafficSource_medium, trafficSource_referralPath, trafficSource_source)
      .transmogrify()

    ////////////////////////////////////////////////////////////////////////////////
    //数据泄露及空值处理
    /////////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////////
    //定义测试模型集
    /////////////////////////////////////////////////////////////////////////////////
    val prediction: FeatureLike[RealNN] =
    RegressionModelSelector
      //      .withCrossValidation()
      .withTrainValidationSplit()
      //RandomForestRegression, LinearRegression, GBTRegression
      .setModelsToTry(LinearRegression)
      .setInput(totals_transactionRevenue, customerFeatures).getOutput()

    val evaluator = Evaluators.Regression()
      .setLabelCol(totals_transactionRevenue)
      .setPredictionCol(prediction)
    ////////////////////////////////////////////////////////////////////////////////
    // WORKFLOW
    /////////////////////////////////////////////////////////////////////////////////
    import spark.implicits._

    val trainDataReader = DataReaders.Simple.csvCase[Customer](path = Option(trainPath), key = v => v.fullVisitorId + "_" + v.sessionId)

    val workflow = new OpWorkflow()
      .setResultFeatures(totals_transactionRevenue, prediction)
      .setReader(trainDataReader)

    val fittedWorkflow = workflow.train()
    println(s"Summary: ${fittedWorkflow.summary()}")

    // Manifest the result features of the workflow
    println("Scoring the model")
    val (dataframe, metrics) = fittedWorkflow.scoreAndEvaluate(evaluator = evaluator)

    println("Transformed dataframe columns:")
    dataframe.columns.foreach(println)
    println("Metrics:")
    println(metrics)
  }
}
