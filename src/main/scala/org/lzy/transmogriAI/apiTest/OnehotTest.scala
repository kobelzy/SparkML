package org.lzy.transmogriAI.apiTest

import com.salesforce.op.features.FeatureBuilder
import com.salesforce.op.stages.impl.feature.OpOneHotVectorizer
import common.SparkUtil
import org.apache.spark.ml.feature.{IndexToString, OneHotEncoder, StringIndexer}
import org.apache.spark.util.SparkUtils
import org.lzy.kaggle.eloRecommendation.Record
import com.salesforce.op.features.types._
import com.salesforce.op.features.{FeatureBuilder, FeatureLike}

  case class ds(id:Int,category:String)
class feature{
}

object OnehotTest {

  implicit val spark=SparkUtil.getSpark()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._
  def main(args: Array[String]): Unit = {
  run()
  }
  val id = FeatureBuilder.ID[ds].extract(_.id.toString.toID).asPredictor
  val category = FeatureBuilder.PickList[ds].extract(_.category.toPickList).asPredictor


  def run()={
    val df = spark.createDataset(Seq( (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))).toDF("id","category").as[ds]
    df.show(false)


    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
    val model = indexer.fit(df)
    val indexed=model.transform(df)

    indexed.show(false)
    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")
          .setDropLast(false)
    val onehotdata=encoder.transform(indexed)
    onehotdata.show(false)

  }
}
