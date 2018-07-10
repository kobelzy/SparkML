package common

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Auther: lzy
  * Description:
  * Date Created by： 17:20 on 2018/6/27
  * Modified By：
  */

class FeatureUtils(spark: SparkSession) {


}

object FeatureUtils {

  def concatTrainAndTest(train_df: DataFrame, test_df: DataFrame, labelCol: String): Dataset[Row] = {
    val baseColumns = test_df.columns
    val test = test_df
      .withColumn(labelCol, lit(1d))
      .select(labelCol, baseColumns: _*)
      .withColumn("df_type", lit(1))
    val train = train_df
      .select(labelCol, baseColumns: _*)
      .withColumn("df_type", lit(0))
    train.union(test)
  }

  def splitTrainAndTest(all_df: DataFrame): (DataFrame, DataFrame) = {
    val train_df = all_df.filter(col("df_type") === 0).drop("df_type")
    val test_df = all_df.filter(col("df_type") === 1).drop("df_type")
    (train_df, test_df)
  }

  /** *
    * 功能实现:专门针对字符串类型进行onehot编码
    * 并添加onehot后缀
    * Author: Lzy
    * Date: 2018/6/27 17:29
    * Param: [column]
    * Return: org.apache.spark.ml.PipelineStage[]
    */
  def stringIndexer_OneHot(column: String): Array[PipelineStage] = {
    var stages = Array[PipelineStage]()
    val string2vector = new StringIndexer()
      .setInputCol(column)
      .setOutputCol(column + "_string2vector")
    stages = stages :+ string2vector
    val onehoter: OneHotEncoder = new OneHotEncoder()
      .setInputCol(column + "_string2vector").setOutputCol(column + "_onehot").setDropLast(false)
    stages = stages :+ onehoter
    stages
  }

  /** *
    * 功能实现:专直接进行onehot编码
    * 并添加onehot后缀
    * Author: Lzy
    * Date: 2018/6/27 17:33
    * Param: [column]
    * Return: org.apache.spark.ml.PipelineStage[]
    */
  def onehot(column: String): OneHotEncoder = {
    var stages = Array[PipelineStage]()
    val onehoter: OneHotEncoder = new OneHotEncoder()
      .setInputCol(column).setOutputCol(column + "_onehot").setDropLast(false)
    onehoter
  }

  /** *
    * 功能实现:针对一组column对其进行onehot编码，并添加onehot后缀
    *
    * Author: Lzy
    * Date: 2018/6/27 17:40
    * Param: [columns]
    * Return: org.apache.spark.ml.PipelineStage[]
    */
  def onehot_allColumns(columns: Array[String]): Array[PipelineStage] = {
    var stages = Array[PipelineStage]()
    columns.map(column => {
      stages = stages :+ onehot(column)
    })
    stages
  }

  def vectorAssemble(columns: Array[String], outColumn: String = "features"): Array[PipelineStage] = {
    var stages = Array[PipelineStage]()
    val vectorAssembler = new VectorAssembler().setInputCols(columns).setOutputCol(outColumn)

    stages = stages :+ vectorAssembler
    stages
  }

  def vectorAssembl(columns: Array[String], outColumn: String = "features") = {
    val vectorAssembler = new VectorAssembler().setInputCols(columns).setOutputCol(outColumn)
    vectorAssembler
  }

  def chiSqSelector(labelColumn: String, features: String, outputCol: String = "features", selectNum: Int): PipelineStage = {
    val selector = new ChiSqSelector().setLabelCol(labelColumn).setFeaturesCol(features).setOutputCol(outputCol)
      .setNumTopFeatures(selectNum)
    selector
  }

  def chiSqSelectorByfdr(labelColumn: String, features: String, outputCol: String = "features", fdr: Double = 0.05): PipelineStage = {
    val selector = new ChiSqSelector().setLabelCol(labelColumn).setFeaturesCol(features).setOutputCol(outputCol)
      .setSelectorType("fdr")
      .setFdr(fdr)

    selector
  }

  def pca(k: Int, inputCol: String, outputCol: String) = {
    val pca = new PCA()
      .setInputCol(inputCol).setOutputCol(outputCol).setK(k)
    pca
  }
}
