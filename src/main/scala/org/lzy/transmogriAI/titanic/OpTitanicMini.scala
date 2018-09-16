package org.lzy.transmogriAI.titanic

import com.salesforce.op._
import com.salesforce.op.features.FeatureBuilder
import com.salesforce.op.features.types._
import com.salesforce.op.readers.DataReaders
import com.salesforce.op.stages.impl.classification._
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * A minimal Titanic Survival example with TransmogrifAI
 */
object OpTitanicMini {

  case class Passenger
  (
    id: Long,
    survived: Double,
    pClass: Option[Long],
    name: Option[String],
    sex: Option[String],
    age: Option[Double],
    sibSp: Option[Long],
    parCh: Option[Long],
    ticket: Option[String],
    fare: Option[Double],
    cabin: Option[String],
    embarked: Option[String]
  )

  def main(args: Array[String]): Unit = {
    LogManager.getLogger("com.salesforce.op").setLevel(Level.ERROR)
    implicit val spark = SparkSession.builder.config(new SparkConf()).getOrCreate()
    import spark.implicits._

    // Read Titanic data as a DataFrame

//    val pathToData = Option(args(0))
//    D:\WorkSpace\ScalaWorkSpace\SparkML\src\main\resources\TitanicDataset\TitanicPassengersTrainData.csv
    val pathToData=Some(System.getenv("TitanicDataset/TitanicPassengersTrainData.csv"))
    println(pathToData)
    val passengersData = DataReaders.Simple.csvCase[Passenger](pathToData, key = _.id.toString).readDataset().toDF()

    // Automated feature engineering
    val (survived, features) = FeatureBuilder.fromDataFrame[RealNN](passengersData, response = "survived")
    val featureVector = features.transmogrify()

    // Automated feature selection
    val checkedFeatures = survived.sanityCheck(featureVector, checkSample = 1.0, removeBadFeatures = true)

    // Automated model selection
    val (pred, raw, prob) = BinaryClassificationModelSelector().setInput(survived, checkedFeatures).getOutput()
    val model = new OpWorkflow().setInputDataset(passengersData).setResultFeatures(pred).train()

    println("Model summary:\n" + model.summaryPretty())
  }

}
