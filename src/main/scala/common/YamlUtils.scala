package common


import org.yaml.snakeyaml.Yaml

import scala.beans.BeanProperty


class YamlUtils extends Serializable {
  @BeanProperty var minInterval: Int  =0
  @BeanProperty var maxInterval: Int = -1
  @BeanProperty var details: java.util.HashMap[String, Object] = null
}
object YamlUtils{
  def getParam={
    val yaml=new Yaml
    val in = this.getClass.getResourceAsStream("/param.yml")
    println("解析")
    yaml.loadAs(in,classOf[YamlUtils])
  }

  def main(args: Array[String]): Unit = {
    val params=YamlUtils.getParam
    println(params.minInterval)
    println(params.details)
    println(params.details.get("age").asInstanceOf[Int])
  }
}
