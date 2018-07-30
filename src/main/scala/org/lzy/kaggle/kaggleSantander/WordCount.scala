package org.lzy.kaggle.kaggleSantander

import java.io.PrintWriter
import scala.io.{BufferedSource, Source}

/**scala -cp SparkML.jar org.lzy.kaggle.kaggleSantander.WordCount C:\Users\taihe\Documents\WeChat Files\kobeliuziyang\Files\examples\gatk\gatk-pipeline.json C:\Users\taihe\Documents\WeChat Files\kobeliuziyang\Files\examples\gatk\result.csv
  * Auther: lzy
  * Description:
  * Date Created by： 17:39 on 2018/7/30
  * Modified By：
  * s
  */

object WordCount {
    def main(args: Array[String]): Unit = {
        val Array(input,output)=args
//        val input="C:\\Users\\taihe\\Documents\\WeChat Files\\kobeliuziyang\\Files\\examples\\gatk\\gatk-pipeline.json"
//        val output="C:\\Users\\taihe\\Documents\\WeChat Files\\kobeliuziyang\\Files\\examples\\gatk\\result.csv"
        val data: BufferedSource = Source.fromFile(input)
        val result: Map[String, Int] = data.getLines().flatMap(_.trim.split(" ")).toArray.map((_,1)).groupBy(_._1).mapValues(_.length)
        println(result)
        val out=new PrintWriter(output)
        for((word,count)<- result){
            out.println(word+"->"+count)
        }
        out.close()
    }
}
