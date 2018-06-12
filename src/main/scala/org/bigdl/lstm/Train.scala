package org.bigdl.lstm

/**
  * Auther: lzy
  * Description:
  * Date Created by： 18:37 on 2018/6/12
  * Modified By：
  */


import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.dataset.PaddingParam
import com.intel.analytics.bigdl.example.treeLSTMSentiment.Utils._
import com.intel.analytics.bigdl.nn.{TimeDistributedCriterion, _}
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.optim._
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.utils.{Engine, LoggerFilter, T}
import org.apache.log4j.{Level => Levle4j, Logger => Logger4j}
import org.apache.spark.SparkContext
import org.slf4j.{Logger, LoggerFactory}

import scala.language.existentials

object Train {
    val log: Logger = LoggerFactory.getLogger(this.getClass)
    LoggerFilter.redirectSparkInfoLogs()
    Logger4j.getLogger("com.intel.analytics.bigdl.optim").setLevel(Levle4j.INFO)

    def main(args: Array[String]): Unit = {
        val params = paramParser.parse(args, TreeLSTMSentimentParam()).get
        train(params)
    }

    def train(param: TreeLSTMSentimentParam): Unit = {
        val DATA_DIR = param.baseDir
        val classNum = 5
        val criterion = TimeDistributedCriterion(ClassNLLCriterion())
        val conf = Engine.createSparkConf()
                .setAppName("Text classification")
                .set("spark.task.maxFailures", "1")
        val sc = new SparkContext(conf)
        Engine.init

        val paddingValue = 1
        val oovChar = 2
        val indexFrom = 3
        val labelPadding = -1f
        val glovePath = s"$DATA_DIR/glove/glove.840B.300d.txt"
        val vocabPath = s"$DATA_DIR/sst/vocab-cased.txt"
        log.info("Start loading embeddings\n")
        val (word2VecTensor, vocab) =
            loadEmbeddingAndVocabulary(sc, glovePath, vocabPath, indexFrom)
        log.info("Finish loading embeddings\n")

        val vocabBC = sc.broadcast(vocab)
        val (trainTreeRDD, trainLabelRDD, trainSentenceRDD) = preProcessData(
            sc,
            vocabBC,
            oovChar,
            s"$DATA_DIR/sst/train/parents.txt",
            s"$DATA_DIR/sst/train/labels.txt",
            s"$DATA_DIR/sst/train/sents.txt")
        log.info(
            s"""
               |train treeRDD count: ${trainTreeRDD.count()}
               |train labelRDD count: ${trainLabelRDD.count()}
               |train sentenceRDD count: ${trainSentenceRDD.count()}
      """.stripMargin)

        val (devTreeRDD, devLabelRDD, devSentenceRDD) = preProcessData(
            sc,
            vocabBC,
            oovChar,
            s"$DATA_DIR/sst/dev/parents.txt",
            s"$DATA_DIR/sst/dev/labels.txt",
            s"$DATA_DIR/sst/dev/sents.txt")
        log.info(
            s"""
               |dev treeRDD count: ${devTreeRDD.count()}
               |dev labelRDD count: ${devLabelRDD.count()}
               |dev sentenceRDD count: ${devSentenceRDD.count()}
      """.stripMargin)

        val trainRDD = toSample(trainTreeRDD, trainLabelRDD, trainSentenceRDD)
        val devRDD = toSample(devTreeRDD, devLabelRDD, devSentenceRDD)

        val optimizer = Optimizer(
            model = TreeLSTMSentiment(word2VecTensor, param.hiddenSize, classNum, param.p),
            sampleRDD = trainRDD,
            criterion = criterion,
            batchSize = param.batchSize,
            featurePaddingParam = PaddingParam[Float](
                paddingTensor =
                        Some(Array(Tensor(T(paddingValue.toFloat)), Tensor(T(-1f, -1f, -1f))))),
            labelPaddingParam = PaddingParam[Float](
                paddingTensor =
                        Some(Array(Tensor(T(-1f))))))

        optimizer
                .setOptimMethod(new Adagrad(
                    learningRate = param.learningRate,
                    weightDecay = param.regRate))
                .setValidation(
                    Trigger.everyEpoch,
                    devRDD,
                    Array(new TreeNNAccuracy()),
                    param.batchSize,
                    PaddingParam[Float](
                        paddingTensor =
                                Some(Array(Tensor(T(paddingValue.toFloat)), Tensor(T(-1f, -1f, -1f))))),
                    PaddingParam[Float](
                        paddingTensor =
                                Some(Array(Tensor(T(-1f))))))
                .setEndWhen(Trigger.maxEpoch(param.epoch))
                .optimize()
        sc.stop()
    }
}