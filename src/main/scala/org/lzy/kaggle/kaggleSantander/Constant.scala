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

  val specialColumns_arr=
/*    Array("f190486d6", "c47340d97", "eeb9cd3aa", "66ace2992", "e176a204a",
    "491b9ee45", "1db387535", "c5a231d81", "0572565c2", "024c577b9",
    "15ace8c9f", "23310aa6f", "9fd594eec", "58e2e02e6", "91f701ba2",
    "adb64ff71", "2ec5b290f", "703885424", "26fc93eb7", "6619d81fc",
    "0ff32eb98", "70feb1494", "58e056e12", "1931ccfdd", "1702b5bf0",
    "58232a6fb", "963a49cdc", "fc99f9426", "241f0f867", "5c6487af1",
    "62e59a501", "f74e8f13d", "fb49e4212", "190db8488", "324921c7b",
    "b43a7cfd5", "9306da53f", "d6bb78916", "fb0f5dbfe", "6eef030c1")*/
    Array("f190486d6","58e2e02e6","eeb9cd3aa","9fd594eec","6eef030c1","58e056e12")

  /*
  对于不同模型的存储位置，路径应该放在这里。

   */

  /**
    * 高相关特征：
    * f190486d6
    */
}
