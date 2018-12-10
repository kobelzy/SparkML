package common

import java.io.File

import org.apache
import org.apache.log4j.PropertyConfigurator
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by Administrator on 2018/7/19.
  */
object SparkUtil {

    def OsName() = {
        System.getProperty("os.name").toLowerCase()
    }

    /***
     * 功能实现:获取sparkSession环境，并使用调用者名字为类名
     *只能在object中调用一次，避免非单例情况的出现
     * Author: Lzy
     * Date: 2018/10/8 10:28
     * Param: []
     * Return: org.apache.spark.sql.SparkSession
     */
    def getSpark() = {
        val conf = getSparkConf()
        //获取调用者的类名，截取掉包名，以及$字符
        val appName = new Throwable().getStackTrace()(1).getClassName().split("\\.").last.stripSuffix("$")
        val spark = SparkSession.builder().config(conf).appName(appName).getOrCreate()
//        spark.sparkContext.setLogLevel("WARN")
        //    config.set("spark.driver.maxResultSize","0")
        conf.set("spark.debug.maxToStringFields", "100")
        conf.set("spark.shuffle.io.maxRetries", "60")
        conf.set("spark.default.parallelism", "54")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        spark
    }

    /** *
      * 功能实现:该方法会自动填充appName，并根据环境自动设置运行状态
      * 但是必须要求使用类直接调用该方法，否则会导致appName不准
      * Author: Lzy
      * Date: 2018/10/8 9:43
      * Param: [conf]
      * Return: org.apache.spark.SparkConf
      */
    def getSparkConf(conf: SparkConf = new SparkConf()): SparkConf = {
        if (System.getProperty("os.name").toLowerCase().indexOf("windows") != -1) {
            //windows操作系统,去除在windows环境下输出的允余信息
//            val path = new File(".").getCanonicalPath
//            System.getProperties.put("hadoop.home.dir", path)
//            //生成了一个空winutils.exe，骗过了骗过环境监测
//            new File("./bin").mkdirs()
//            new File("./bin/winutils.exe").createNewFile()
            conf.setMaster("local[*]")
        }
        //获取调用者的类名，截取掉包名，以及$字符
        val appName = new Throwable().getStackTrace()(1).getClassName().split("\\.").last.stripSuffix("$")
        conf.setAppName(appName)
    }



}
