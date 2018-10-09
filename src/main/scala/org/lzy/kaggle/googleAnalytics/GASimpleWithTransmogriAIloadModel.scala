package org.lzy.kaggle.googleAnalytics

import com.salesforce.op.{OpWorkflow, OpWorkflowModel}
import common.SparkUtil

/**
  * Auther: lzy
  * Description:
  * Date Created by： 9:23 on 2018/10/9
  * Modified By：
  */

object GASimpleWithTransmogriAIloadModel {
    def main(args: Array[String]): Unit = {
        val spark=SparkUtil.getSpark()
        import spark.implicits._

        val model=new OpWorkflow().loadModel(Constants.basePath+"model/bestModel")
//        model.score()
    }
}
