package common

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.{ChiSqSelector, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

/**
  * Auther: lzy
  * Description:
  * Date Created by： 17:20 on 2018/6/27
  * Modified By：
  */

class FeatureUtils(spark: SparkSession) {
import spark.implicits._



}

object FeatureUtils {
    /***
      * 功能实现:专门针对字符串类型进行onehot编码
      *并添加onehot后缀
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

    /***
      * 功能实现:专直接进行onehot编码
      *并添加onehot后缀
      * Author: Lzy
      * Date: 2018/6/27 17:33
      * Param: [column]
      * Return: org.apache.spark.ml.PipelineStage[]
      */
    def onehot(column: String): OneHotEncoder = {
        var stages = Array[PipelineStage]()
        val onehoter: OneHotEncoder = new OneHotEncoder()
                .setInputCol(column ).setOutputCol(column + "_onehot").setDropLast(false)
        onehoter
    }
    /***
      * 功能实现:针对一组column对其进行onehot编码，并添加onehot后缀
      *
      * Author: Lzy
      * Date: 2018/6/27 17:40
      * Param: [columns]
      * Return: org.apache.spark.ml.PipelineStage[]
      */
    def onehot_allColumns(columns: Array[String]): Array[PipelineStage] ={
        var stages = Array[PipelineStage]()
        columns.map(column=>{
            stages = stages :+ onehot(column)
        })
        stages
    }

    def vectorAssemble(columns:Array[String],outColumn:String="features"):Array[PipelineStage]={
        var stages = Array[PipelineStage]()
        val vectorAssembler=new VectorAssembler().setInputCols(columns).setOutputCol(outColumn)

        stages = stages :+ vectorAssembler
        stages
    }

    def chiSqSelector(labelColumn:String,features:String,outputCol:String="features",selectNum:Int):PipelineStage={
        val selector=new ChiSqSelector().setLabelCol(labelColumn).setFeaturesCol(features).setOutputCol(outputCol)
          .setNumTopFeatures(selectNum)
        selector
    }
    def chiSqSelectorByfdr(labelColumn:String,features:String,outputCol:String="features",fdr:Double=0.05):PipelineStage={
        val selector=new ChiSqSelector().setLabelCol(labelColumn).setFeaturesCol(features).setOutputCol(outputCol)
          .setSelectorType("fdr")
          .setFdr(fdr)

        selector
    }
}
