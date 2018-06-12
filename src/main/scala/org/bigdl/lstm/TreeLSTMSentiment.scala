package org.bigdl.lstm

/**
  * Auther: lzy
  * Description:
  * Date Created by： 18:38 on 2018/6/12
  * Modified By：
  */


import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.nn._
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.tensor.Tensor

import scala.language.existentials

object TreeLSTMSentiment {
    def apply(
                     word2VecTensor: Tensor[Float],
                     hiddenSize: Int,
                     classNum: Int,
                     p: Double = 0.5
             ): Module[Float] = {
        val vocabSize = word2VecTensor.size(1)
        val embeddingDim = word2VecTensor.size(2)
        val embedding = LookupTable(vocabSize, embeddingDim)
        embedding.weight.set(word2VecTensor)
        embedding.setScaleW(2)

        val treeLSTMModule = Sequential()
                .add(BinaryTreeLSTM(
                    embeddingDim, hiddenSize, withGraph = true))
                .add(TimeDistributed(Dropout(p)))
                .add(TimeDistributed(Linear(hiddenSize, classNum)))
                .add(TimeDistributed(LogSoftMax()))

        Sequential()
                .add(MapTable(Squeeze(3)))
                .add(ParallelTable()
                        .add(embedding)
                        .add(Identity()))
                .add(treeLSTMModule)
    }
}
