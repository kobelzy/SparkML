

package org.lzy.transmogriAI.titanic

import com.esotericsoftware.kryo.Kryo
import com.salesforce.op.utils.kryo.OpKryoRegistrator
import org.lzy.transmogriAI.titanic.OpTitanicMini.Passenger



class TitanicKryoRegistrator extends OpKryoRegistrator {

  override def registerCustomClasses(kryo: Kryo): Unit = {
    doClassRegistration(kryo)(classOf[Passenger])


  }

}
