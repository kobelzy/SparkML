
package org.lzy.transmogriAI

import com.salesforce.op._
import com.salesforce.op.evaluators.Evaluators
import com.salesforce.op.features.FeatureBuilder
import com.salesforce.op.features.types._
import com.salesforce.op.readers.DataReaders
import com.salesforce.op.stages.impl.classification.BinaryClassificationModelSelector
import com.salesforce.op.stages.impl.classification.BinaryClassificationModelsToTry._
import common.SparkUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Define a case class corresponding to our data file (nullable columns must be Option types)
  *
  * @param id       passenger id
  * @param survived 1: survived, 0: did not survive
  * @param pClass   passenger class
  * @param name     passenger name
  * @param sex      passenger sex (male/female)
  * @param age      passenger age (one person has a non-integer age so this must be a double)
  * @param sibSp    number of siblings/spouses traveling with this passenger
  * @param parCh    number of parents/children traveling with this passenger
  * @param ticket   ticket id string
  * @param fare     ticket price
  * @param cabin    cabin id string
  * @param embarked location where passenger embarked
  */
case class Passenger
(
  id: Int,
  survived: Int,
  pClass: Option[Int],
  name: Option[String],
  sex: Option[String],
  age: Option[Double],
  sibSp: Option[Int],
  parCh: Option[Int],
  ticket: Option[String],
  fare: Option[Double],
  cabin: Option[String],
  embarked: Option[String]
)

/**
  * A simplified TransmogrifAI example classification app using the Titanic dataset
  */
object OpTitanicSimple {

  /**
    * Run this from the command line with
    * ./gradlew sparkSubmit -Dmain=com.salesforce.hw.OpTitanicSimple -Dargs=/full/path/to/csv/file
    */
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkUtil.getSpark()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    // Read Titanic data as a DataFrame
    val csvFilePath = ClassLoader.getSystemResource("TransmogrifData/TitanicPassengersTrainData.csv").toString
    val testPath = ClassLoader.getSystemResource("TransmogrifData/TitanicPassengersTestData.csv").toString
    println(s"Using user-supplied CSV file path: $csvFilePath")

    // Define a way to read data into our Passenger class from our CSV file
    val trainDataReader = DataReaders.Simple.csvCase[Passenger](
      path = Option(csvFilePath),
      key = _.id.toString
    )
    trainDataReader.readDataset().show(false)
    require(false)
    val testDataReader = DataReaders.Simple.csvCase[Passenger](
      path = Option(testPath),
      key = _.id.toString
    )
    ////////////////////////////////////////////////////////////////////////////////
    // RAW FEATURE DEFINITIONS
    /////////////////////////////////////////////////////////////////////////////////

    // Define features using the OP types based on the data
    val survived = FeatureBuilder.RealNN[Passenger].extract(_.survived.toRealNN).asResponse
    val pClass = FeatureBuilder.PickList[Passenger].extract(_.pClass.map(_.toString).toPickList).asPredictor
    val name = FeatureBuilder.Text[Passenger].extract(_.name.toText).asPredictor
    val sex = FeatureBuilder.PickList[Passenger].extract(_.sex.map(_.toString).toPickList).asPredictor
    val age = FeatureBuilder.Real[Passenger].extract(_.age.toReal).asPredictor
    val sibSp = FeatureBuilder.Integral[Passenger].extract(_.sibSp.toIntegral).asPredictor
    val parCh = FeatureBuilder.Integral[Passenger].extract(_.parCh.toIntegral).asPredictor
    val ticket = FeatureBuilder.PickList[Passenger].extract(_.ticket.map(_.toString).toPickList).asPredictor
    val fare = FeatureBuilder.Real[Passenger].extract(_.fare.toReal).asPredictor
    val cabin = FeatureBuilder.PickList[Passenger].extract(_.cabin.map(_.toString).toPickList).asPredictor
    val embarked = FeatureBuilder.PickList[Passenger].extract(_.embarked.map(_.toString).toPickList).asPredictor


    ////////////////////////////////////////////////////////////////////////////////
    // TRANSFORMED FEATURES
    /////////////////////////////////////////////////////////////////////////////////

    // Do some basic feature engineering using knowledge of the underlying dataset
    val familySize = sibSp + parCh + 1
    val estimatedCostOfTickets = familySize * fare
    val pivotedSex = sex.pivot()
    val normedAge = age.fillMissingWithMean().zNormalize()
    val ageGroup = age.map[PickList](_.value.map(v => if (v > 18) "adult" else "child").toPickList)

    // Define a feature of type vector containing all the predictors you'd like to use
    val passengerFeatures = Seq(
      pClass, name, age, sibSp, parCh, ticket,
      cabin, embarked, familySize, estimatedCostOfTickets,
      pivotedSex, ageGroup
    ).transmogrify()

    // Optionally check the features with a sanity checker
    val sanityCheck = true
    val finalFeatures = if (sanityCheck) survived.sanityCheck(passengerFeatures) else passengerFeatures

    // Define the model we want to use (here a simple logistic regression) and get the resulting output
    val prediction =
      BinaryClassificationModelSelector.withTrainValidationSplit(
        modelTypesToUse = Seq(OpLogisticRegression)
      ).setInput(survived, finalFeatures).getOutput()

    val evaluator = Evaluators.BinaryClassification().setLabelCol(survived).setPredictionCol(prediction)

    ////////////////////////////////////////////////////////////////////////////////
    // WORKFLOW
    /////////////////////////////////////////////////////////////////////////////////


    // Define a new workflow and attach our data reader
    val workflow: OpWorkflow =
      new OpWorkflow()
        .setResultFeatures(prediction)
        .setReader(trainDataReader)

    // Fit the workflow to the data
    val fittedWorkflow: OpWorkflowModel = workflow.train()
    //        fittedWorkflow.score(Option(csvFilePath)).show(false)
    fittedWorkflow.computeDataUpTo(survived).show(false)
    println(fittedWorkflow.summary())
    println(fittedWorkflow.summaryPretty())
    //        println(s"Summary: ${fittedWorkflow.summary()}")
    //
    //        // Manifest the result features of the workflow
    //        println("Scoring the model")
    //        val (dataframe, metrics) = fittedWorkflow
    ////                .setReader(testDataReader)
    //        .scoreAndEvaluate(evaluator = evaluator)
    //        println("Transformed dataframe columns:")
    //        dataframe.show(false)
    //        println("Metrics:")
    //        println(metrics)
    //        println("-----------")
    //        val dataframe2 = fittedWorkflow
    //                .setReader(testDataReader)
    //                .score()
    //        println("Transformed dataframe columns:")
    //        dataframe2.show(false)
  }
}