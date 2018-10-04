package org.lzy.transmogriAI.boston

import com.salesforce.op._
import com.salesforce.op.evaluators.Evaluators
import com.salesforce.op.readers.CustomReader
import com.salesforce.op.stages.impl.regression.RegressionModelsToTry.OpLinearRegression
import com.salesforce.op.stages.impl.regression._
import com.salesforce.op.stages.impl.tuning.DataSplitter
import com.salesforce.op.utils.kryo.OpKryoRegistrator
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}


/**
  * TransmogrifAI Regression example on the Boston Dataset
  */
object OpBoston extends OpAppWithRunner with BostonFeatures {
  val randomSeed = 112233L
  implicit val spark = SparkSession.builder
    .master("local[*]")
    .config(new SparkConf()).getOrCreate()
  ////////////////////////////////////////////////////////////////////////////////
  // READERS DEFINITION
  /////////////////////////////////////////////////////////////////////////////////
  val trainingReader = new CustomReader[BostonHouse](key = _.rowId.toString) {
    def readFn(params: OpParams)(implicit spark: SparkSession): Either[RDD[BostonHouse], Dataset[BostonHouse]] = {
      val Array(train, _) = customRead(Some(getFinalReadPath(params)), spark).randomSplit(weights = Array(0.9, 0.1),
        seed = randomSeed)
      Left(train)
    }
  }
  val scoringReader: CustomReader[BostonHouse] {
    def readFn(params: OpParams)
              (implicit spark: SparkSession): Either[RDD[BostonHouse], Dataset[BostonHouse]]
  }
  = new CustomReader[BostonHouse](key = _.rowId.toString) {
    def readFn(params: OpParams)(implicit spark: SparkSession): Either[RDD[BostonHouse], Dataset[BostonHouse]] = {
      val Array(_, test) = customRead(Some(getFinalReadPath(params)), spark).randomSplit(weights = Array(0.9, 0.1),
        seed = randomSeed)
      Left(test)
    }
  }
  //  val lr=new OpLinearRegression()
  //  val gbdt = new OpGBTRegressor()
  //      val rf = new OpRandomForestRegressor()
  //  val models = Seq(
  //      lr->new ParamGridBuilder()
  //        .addGrid(lr.maxIter,Array(50))
  //          .addGrid(lr.epsilon,Array(1.0))
  //        .build()
  //    gbdt -> new ParamGridBuilder()
  //            .addGrid(gbdt.seed, Array(randomSeed))
  //            .addGrid(gbdt.elasticNetParam, Array(0.01))
  //      .build()
  //            rf -> new ParamGridBuilder()
  //              .addGrid(rf.maxDepth, Array(5, 10))
  //              .addGrid(rf.minInstancesPerNode, Array(10, 20, 30))
  //              .addGrid(rf.seed, Array(randomSeed))
  //              .build()
  //  )

  val houseFeatures = Seq(crim, zn, indus, chas, nox, rm, age, dis, rad, tax, ptratio, b, lstat).transmogrify()
  val splitter = DataSplitter(seed = randomSeed, reserveTestFraction = 0.1)

  val prediction = RegressionModelSelector
    .withTrainValidationSplit(
      dataSplitter = Some(splitter), seed = randomSeed
      , modelTypesToUse = Seq(OpLinearRegression)
      //      , modelsAndParameters = models
    )
    .setInput(medv, houseFeatures)
    .getOutput()


  ////////////////////////////////////////////////////////////////////////////////
  // WORKFLOW DEFINITION
  /////////////////////////////////////////////////////////////////////////////////
  val workflow = new OpWorkflow().setResultFeatures(prediction)
  val evaluator = Evaluators.Regression().setLabelCol(medv).setPredictionCol(prediction)

  override def kryoRegistrator: Class[_ <: OpKryoRegistrator] = classOf[BostonKryoRegistrator]

  def customRead(path: Option[String], spark: SparkSession): RDD[BostonHouse] = {
    require(path.isDefined, "The path is not set")
    val myFile = spark.sparkContext.textFile(path.get)

    myFile.filter(_.nonEmpty).zipWithIndex.map { case (x, number) =>
      val words = x.replaceAll("\\s+", " ").replaceAll(s"^\\s+(?m)", "").replaceAll(s"(?m)\\s+$$", "").split(" ")
      BostonHouse(number.toInt, words(0).toDouble, words(1).toDouble, words(2).toDouble, words(3), words(4).toDouble,
        words(5).toDouble, words(6).toDouble, words(7).toDouble, words(8).toInt, words(9).toDouble,
        words(10).toDouble, words(11).toDouble, words(12).toDouble, words(13).toDouble)
    }
  }

  def runner(opParams: OpParams): OpWorkflowRunner =
    new OpWorkflowRunner(
      workflow = workflow,
      trainingReader = trainingReader,
      scoringReader = scoringReader,
      evaluationReader = Option(trainingReader),
      evaluator = Option(evaluator),
      scoringEvaluator = None,
      featureToComputeUpTo = Option(houseFeatures)
    )

}
