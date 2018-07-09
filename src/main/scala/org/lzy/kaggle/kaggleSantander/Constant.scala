package org.lzy.kaggle.kaggleSantander

/**
  * Created by Administrator on 2018/7/3.
  */
object Constant {
  //  val basePath = "E:\\dataset\\Kaggle_Santander\\"
  val basePath = "hdfs://10.95.3.172:9000/user/lzy/Kaggle_Santander/"

  val featureFilterColumns_arr=Array("id","target")
  val lableCol="target"
  val predictionCol=""
  val featuresCol="features"
}
