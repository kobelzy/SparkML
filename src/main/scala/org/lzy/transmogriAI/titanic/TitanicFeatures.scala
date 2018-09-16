package org.lzy.transmogriAI.titanic

import com.salesforce.op.features.FeatureBuilder
import com.salesforce.op.features.types._
import org.lzy.transmogriAI.Passenger

trait TitanicFeatures extends Serializable {

  val survived = FeatureBuilder.RealNN[Passenger].extract(_.survived.toDouble.toRealNN).asResponse

  val pClass = FeatureBuilder.PickList[Passenger].extract(d => Option(d.pClass).map(_.toString).toPickList).asPredictor // scalastyle:off

  val name = FeatureBuilder.Text[Passenger].extract(d => Option(d.name).map(_.toString).toText).asPredictor

  val sex = FeatureBuilder.PickList[Passenger].extract(d => Option(d.sex).map(_.toString).toPickList).asPredictor

  val age = FeatureBuilder.Real[Passenger].extract(d => Option(Double.unbox(d.age)).toReal).asPredictor

  val sibSp = FeatureBuilder.PickList[Passenger].extract(d => Option(d.sibSp).map(_.toString).toPickList).asPredictor

  val parch = FeatureBuilder.PickList[Passenger].extract(d => Option(d.parCh).map(_.toString).toPickList).asPredictor

  val ticket = FeatureBuilder.PickList[Passenger].extract(d => Option(d.ticket).map(_.toString).toPickList).asPredictor

  val fare = FeatureBuilder.Real[Passenger].extract(d => Option(Double.unbox(d.fare)).toReal).asPredictor

  val cabin = FeatureBuilder.PickList[Passenger].extract(d => Option(d.cabin).map(_.toString).toPickList).asPredictor

  val embarked = FeatureBuilder.PickList[Passenger].extract(d => Option(d.embarked).map(_.toString).toPickList).asPredictor

}
