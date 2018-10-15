package org.lzy.kaggle.googleAnalytics

import java.text.SimpleDateFormat

/**
  * Auther: lzy
  * Description:
  * Date Created by： 9:34 on 2018/10/9
  * Modified By：
  */

object Constants {
  var basePath = "hdfs://10.95.3.172:9000/user/lzy/GoogleAnalyse/"
  if (System.getProperty("os.name").toLowerCase().indexOf("windows") != -1) {
//    basePath = "D:/Dataset/GoogleAnalytics/"
         basePath = "hdfs://10.95.3.172:9000/user/lzy/GoogleAnalyse/"
  }

  val trainPath = basePath + "source/extracted_fields_train.csv"
  val testPath = basePath + "source/extracted_fields_test.csv"
}
