package org.lzy

    import com.salesforce.op._
    import com.salesforce.op.readers._
    import com.salesforce.op.features._
    import com.salesforce.op.features.types._
    import com.salesforce.op.stages.impl.classification._
    import com.salesforce.op.test.Passenger
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
/**
  * Created by Administrator on 2018/9/15.
  */
object TransmogrifAITest {
  /*def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession.builder.config(new SparkConf()).getOrCreate()
    import spark.implicits._

    val pathToData=Some("")
    // Read Titanic data as a DataFrame
    val passengersData = DataReaders.Simple.csvCase[Passenger](path = pathToData).readDataset().toDF()

    // Extract response and predictor Features
    val (survived, predictors) = FeatureBuilder.fromDataFrame[RealNN](passengersData, response = "survived")

    // Automated feature engineering
    val featureVector = predictors.transmogrify()

    // Automated feature validation and selection
    val checkedFeatures = survived.sanityCheck(featureVector, removeBadFeatures = true)

    // Automated model selection
    val pred:(FeatureLike[RealNN], FeatureLike[OPVector], FeatureLike[OPVector]) = BinaryClassificationModelSelector().setInput(survived, checkedFeatures).getOutput()

    // Setting up a TransmogrifAI workflow and training the model
    val model: OpWorkflowModel = new OpWorkflow()
      .setInputDataset(passengersData)
      .setResultFeatures(pred._1,pred._2)

      .train()

    println("Model summary:\n" + model.summaryPretty())
  }*/
}
