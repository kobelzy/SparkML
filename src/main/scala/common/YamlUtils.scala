package common


import org.yaml.snakeyaml.Yaml

import scala.beans.BeanProperty


class YamlUtils extends Serializable {
  @BeanProperty var minInterval: Int = 0
  @BeanProperty var maxInterval: Int = -1
  @BeanProperty var details: java.util.HashMap[String, Object] = null
}

object YamlUtils {
  val yaml = new Yaml
  val in = this.getClass.getResourceAsStream("/param.yml")
  val yamlUtils = yaml.loadAs(in, classOf[YamlUtils])
  println("解析")



  def main(args: Array[String]): Unit = {
    val params = YamlUtils.yamlUtils
    println(params.minInterval)
    println(YamlUtils.yamlUtils.details)
    println(YamlUtils.yamlUtils.details.get("age").asInstanceOf[Int])
  }
}
