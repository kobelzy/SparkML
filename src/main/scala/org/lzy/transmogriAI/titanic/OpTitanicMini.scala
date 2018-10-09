package org.lzy.transmogriAI.titanic

import com.salesforce.op._
import com.salesforce.op.features.FeatureBuilder
import com.salesforce.op.features.types._
import com.salesforce.op.readers.DataReaders
import com.salesforce.op.stages.impl.classification.BinaryClassificationModelsToTry.{OpLogisticRegression, OpRandomForestClassifier}
import com.salesforce.op.stages.impl.classification._
import common.SparkUtil
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * A minimal Titanic Survival example with TransmogrifAI
  */
object OpTitanicMini {

  def main(args: Array[String]): Unit = {
    LogManager.getLogger("com.salesforce.op").setLevel(Level.ERROR)
    implicit val spark = SparkUtil.getSpark()
    import spark.implicits._

    // Read Titanic data as a DataFrame
    val pathToData = Option(ClassLoader.getSystemResource("TransmogrifData/TitanicPassengersTrainData.csv").toString)
    val passengersData = DataReaders.Simple.csvCase[Passenger](pathToData, key = _.id.toString).readDataset().toDF()

    // Automated feature engineering
    val (survived, features) = FeatureBuilder.fromDataFrame[RealNN](passengersData, response = "survived")
    val featureVector = features.transmogrify()

    // Automated feature selection
    val checkedFeatures = survived.sanityCheck(featureVector, checkSample = 1.0, removeBadFeatures = true)

    // Automated model selection
    val prediction = BinaryClassificationModelSelector
      .withCrossValidation(modelTypesToUse = Seq(OpLogisticRegression, OpRandomForestClassifier))
      .setInput(survived, checkedFeatures).getOutput()
    val model:OpWorkflowModel = new OpWorkflow().setInputDataset(passengersData).setResultFeatures(prediction).train()
    model.score().show()


    println("Model summary:\n" + model.summaryPretty())

  }

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

}
