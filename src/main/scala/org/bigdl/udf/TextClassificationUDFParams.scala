package org.bigdl.udf

/**
  * Auther: lzy
  * Description:
  * Date Created by： 18:45 on 2018/6/12
  * Modified By：
  */


import com.intel.analytics.bigdl.example.utils.AbstractTextClassificationParams

/**
  * Text classification udf parameters
  */
case class TextClassificationUDFParams(
                                              override val baseDir: String = "./",
                                              override val maxSequenceLength: Int = 1000,
                                              override val maxWordsNum: Int = 20000,
                                              override val trainingSplit: Double = 0.8,
                                              override val batchSize: Int = 128,
                                              override val embeddingDim: Int = 100,
                                              override val partitionNum: Int = 4,
                                              modelPath: Option[String] = None,
                                              checkpoint: Option[String] = None,
                                              testDir: String = "./")
        extends AbstractTextClassificationParams