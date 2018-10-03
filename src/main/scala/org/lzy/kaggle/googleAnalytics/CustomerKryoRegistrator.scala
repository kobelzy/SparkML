package org.lzy.kaggle.googleAnalytics

import com.esotericsoftware.kryo.Kryo
import com.salesforce.op.utils.kryo.OpKryoRegistrator

/**
  * Created by Administrator on 2018/10/3.
  */
class CustomerKryoRegistrator extends OpKryoRegistrator {
  override def registerCustomClasses(kryo: Kryo): Unit = {
    doClassRegistration(kryo)(classOf[Customer])
  }
}
