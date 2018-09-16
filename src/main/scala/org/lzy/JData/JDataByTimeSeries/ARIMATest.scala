package org.lzy.JData.JDataByTimeSeries

import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.Vectors
import org.spark_project.dmg.pmml.ARIMA
/**
  * Created by Administrator on 2018/5/27.
  */
object ARIMATest {
  def main(args: Array[String]): Unit = {
    // The dataset is sampled from an ARIMA(1, 0, 1) model generated in R.
    val lines = scala.io.Source.fromFile("D:\\WorkSpace\\scala\\SparkML\\src\\main\\resources\\data2\\R_ARIMA_DataSet1.csv").getLines()
    val ts = Vectors.dense(lines.map(_.toDouble).toArray)
    val ts2 = Vectors.dense(ts.toArray.take(5).toArray)
    val naVector=Vectors.dense(Array[Double]())
    println(ts.toArray.mkString(","))
    println(ts2.toArray.mkString(","))
    println(ts2.size)
//    val arimaModel = ARIMA.fitModel(1, 0, 1, ts)
//    println("coefficients: " + arimaModel.coefficients.mkString(","))
//    val forecast = arimaModel.forecast(naVector, 1)
//    println("forecast of next 20 observations: " + forecast.toArray.mkString(","))
  }
}
