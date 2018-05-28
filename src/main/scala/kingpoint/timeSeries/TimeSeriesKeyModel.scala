package kingpoint.timeSeries

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cloudera.sparkts.TimeSeriesRDD
import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
 * 时间序列模型(处理的数据多一个key列)
 * Created by llq on 2017/5/3.
 */
class TimeSeriesKeyModel {
  //预测后面N个值
  private var predictedN=1
  //存放的表名字
  private var outputTableName="time_series.timeseries_output"

  def this(predictedN:Int,outputTableName:String){
    this()
    this.predictedN=predictedN
    this.outputTableName=outputTableName
  }

  /**
   * 实现Arima模型，处理数据是多一个key列
   * @param trainTsrdd
   * @return
   */
  def  arimaModelTrainKey(trainTsrdd:TimeSeriesRDD[String]):  (RDD[(String,Vector)],RDD[(String,(String,(String,String,String),String,String))]) ={
    /***参数设置******/
    val predictedN=this.predictedN

    /***创建arima模型***/
    //创建和训练arima模型.其RDD格式为(ArimaModel,Vector)
    val arimaAndVectorRdd=trainTsrdd.map{line=>
      line match {
        case (key,denseVector)=>
          (key,ARIMA.autoFit(denseVector),denseVector)
      }
    }

    /**参数输出:p,d,q的实际值和其系数值、最大似然估计值、aic值**/
    val coefficients=arimaAndVectorRdd.map{line=>
      line match{
        case (key,arimaModel,denseVector)=>{
          (key,(arimaModel.coefficients.mkString(","),
            (arimaModel.p.toString,
              arimaModel.d.toString,
              arimaModel.q.toString),
            arimaModel.logLikelihoodCSS(denseVector).toString,
            arimaModel.approxAIC(denseVector).toString))
        }
      }
    }
    coefficients.collect().map{_ match{
      case (key,(coefficients,(p,d,q),logLikelihood,aic))=>
        println(key+" coefficients:"+coefficients+"=>"+"(p="+p+",d="+d+",q="+q+")")
    }}


    /***预测出后N个的值*****/
    val forecast = arimaAndVectorRdd.map{row=>
      row match{
        case (key,arimaModel,denseVector)=>{
          (key,arimaModel.forecast(denseVector, predictedN))
        }
      }
    }
    //取出预测值
    val forecastValue=forecast.map{
      _ match{
        case (key,value)=>{
          val partArray=value.toArray.mkString(",").split(",")
          var forecastArrayBuffer=new ArrayBuffer[Double]()
          var i=partArray.length-predictedN
          while(i<partArray.length){
            forecastArrayBuffer+=partArray(i).toDouble
            i=i+1
          }
          (key,Vectors.dense(forecastArrayBuffer.toArray))
        }
      }
    }
    println("Arima forecast of next "+predictedN+" observations:")
    forecastValue.foreach(println)

    return (forecastValue,coefficients)
  }

  /**
   * Arima模型评估参数的保存
   * coefficients、（p、d、q）、logLikelihoodCSS、Aic、mean、variance、standard_deviation、max、min、range、count
   * @param coefficients
   * @param forecastValue
   * @param spark
   */
  def arimaModelKeyEvaluationSave(coefficients:RDD[(String,(String,(String,String,String),String,String))],forecastValue:RDD[(String,Vector)],spark:SparkSession): Unit ={
    /**把vector转置**/
    val forecastRdd=forecastValue.map{
      _ match{
        case (key,forecast)=>forecast.toArray
      }
    }
    // Split the matrix into one number per line.
    val byColumnAndRow = forecastRdd.zipWithIndex.flatMap {
      case (row, rowIndex) => row.zipWithIndex.map {
        case (number, columnIndex) => columnIndex -> (rowIndex, number)
      }
    }
    // Build up the transposed matrix. Group and sort by column index first.
    val byColumn = byColumnAndRow.groupByKey.sortByKey().values
    // Then sort by row index.
    val transposed = byColumn.map {
      indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
    }
    val summary=Statistics.colStats(transposed.map(value=>Vectors.dense(value(0))))

    /**统计求出预测值的均值、方差、标准差、最大值、最小值、极差、数量等;合并模型评估数据+统计值**/
    //评估模型的参数+预测出来数据的统计值
/*    val evaluation=coefficients.join(forecastValue.map{
      _ match{
        case (key,forecast)=>{
          (key,(summary.mean.toArray(0).toString,
            summary.variance.toArray(0).toString,
            math.sqrt(summary.variance.toArray(0)).toString,
            summary.max.toArray(0).toString,
            summary.min.toArray(0).toString,
            (summary.max.toArray(0)-summary.min.toArray(0)).toString,
            summary.count.toString))
        }
      }
    })

    val evaluationRddRow=evaluation.map{
      _ match{
        case (key,((coefficients,pdq,logLikelihoodCSS,aic),(mean,variance,standardDeviation,max,min,range,count)))=>{
          Row(coefficients,pdq.toString,logLikelihoodCSS,aic,mean,variance,standardDeviation,max,min,range,count)
        }
      }
    }
    //形成评估dataframe
    val schemaString="coefficients,pdq,logLikelihoodCSS,aic,mean,variance,standardDeviation,max,min,range,count"
    val schema=StructType(schemaString.split(",").map(fileName=>StructField(fileName,StringType,true)))
    val evaluationDf=spark.createDataFrame(evaluationRddRow,schema)

    println("Evaluation in Arima:")
    evaluationDf.show()
    //存进hive中
    evaluationDf.write.mode(SaveMode.Overwrite).saveAsTable(outputTableName+"_arima_evaluation")*/
  }


  /**
   * 去掉row开头数据的括号和结尾的括号
   */
  private def numChoose(word:String):String={
    val numPattern="\\d*(\\.?)\\d*".r
    numPattern.findAllIn(word).mkString("")
  }

  /**
   * 实现holtwinters模型，处理的数据多一个key列
   * @param trainTsrdd
   * @param period
   * @param holtWintersModelType
   * @return
   */
  def holtWintersModelTrainKey(trainTsrdd:TimeSeriesRDD[String],period:Int,holtWintersModelType:String): (RDD[(String,Vector)],RDD[(String,Double)]) ={
    /***参数设置******/
    //往后预测多少个值
    val predictedN=this.predictedN

    /***创建HoltWinters模型***/
    //创建和训练HoltWinters模型.其RDD格式为(HoltWintersModel,Vector)
    val holtWintersAndVectorRdd=trainTsrdd.map{line=>
      line match {
        case (key,denseVector)=>
          (key,HoltWinters.fitModel(denseVector,period,holtWintersModelType),denseVector)
      }
    }

    /***预测出后N个的值*****/
    //构成N个预测值向量，之后导入到holtWinters的forcast方法中
    val predictedArrayBuffer=new ArrayBuffer[Double]()
    var i=0
    while(i<predictedN){
      predictedArrayBuffer+=i
      i=i+1
    }
    val predictedVectors=Vectors.dense(predictedArrayBuffer.toArray)

    //预测
    val forecast = holtWintersAndVectorRdd.map{row=>
      row match{
        case (key,holtWintersModel,denseVector)=>{
          (key,holtWintersModel.forecast(denseVector, predictedVectors))
        }
      }
    }
    println("HoltWinters forecast of next "+predictedN+" observations:")
    forecast.foreach(println)

    /**holtWinters模型评估度量：SSE和方差**/
    val sse=holtWintersAndVectorRdd.map{row=>
      row match{
        case (key,holtWintersModel,denseVector)=>{
          (key,holtWintersModel.sse(denseVector))
        }
      }
    }
    return (forecast,sse)
  }

  /**
   * HoltWinters模型评估参数的保存
   * sse、mean、variance、standard_deviation、max、min、range、count
   * @param sse
   * @param forecastValue
   * @param spark
   */
  def holtWintersModelKeyEvaluationSave(sse:RDD[(String,Double)],forecastValue:RDD[(String,Vector)],spark:SparkSession): Unit ={
    /**把vector转置**/
    val forecastRdd=forecastValue.map{
      _ match{
        case (key,forecast)=>forecast.toArray
      }
    }
    // Split the matrix into one number per line.
    val byColumnAndRow = forecastRdd.zipWithIndex.flatMap {
      case (row, rowIndex) => row.zipWithIndex.map {
        case (number, columnIndex) => columnIndex -> (rowIndex, number)
      }
    }
    // Build up the transposed matrix. Group and sort by column index first.
    val byColumn = byColumnAndRow.groupByKey.sortByKey().values
    // Then sort by row index.
    val transposed = byColumn.map {
      indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
    }
    val summary=Statistics.colStats(transposed.map(value=>Vectors.dense(value(0))))

    /**统计求出预测值的均值、方差、标准差、最大值、最小值、极差、数量等;合并模型评估数据+统计值**/
    //评估模型的参数+预测出来数据的统计值
/*    val evaluation=sse.join(forecastValue.map{
      _ match{
        case (key,forecast)=>{
          (key,(summary.mean.toArray(0).toString,
            summary.variance.toArray(0).toString,
            math.sqrt(summary.variance.toArray(0)).toString,
            summary.max.toArray(0).toString,
            summary.min.toArray(0).toString,
            (summary.max.toArray(0)-summary.min.toArray(0)).toString,
            summary.count.toString))
        }
      }
    })

    val evaluationRddRow=evaluation.map{
      _ match{
        case (key,(sse,(mean,variance,standardDeviation,max,min,range,count)))=>{
          Row(sse.toString,mean,variance,standardDeviation,max,min,range,count)
        }
      }
    }
    //形成评估dataframe
    val schemaString="sse,mean,variance,standardDeviation,max,min,range,count"
    val schema=StructType(schemaString.split(",").map(fileName=>StructField(fileName,StringType,true)))
    val evaluationDf=spark.createDataFrame(evaluationRddRow,schema)

    println("Evaluation in HoltWinters:")
    evaluationDf.show()

    //存进hive中
    evaluationDf.write.mode(SaveMode.Overwrite).saveAsTable(outputTableName+"_holtwinters_evaluation")*/
  }

  /**
   * 批量生成日期（具体到月份的），用来保存
   * 格式为yyyyMM
   * @param predictedN
   * @param startTime
   * @param endTime
   */
  private def productStartDatePredictDate(predictedN:Int,startTime:String,endTime:String): ArrayBuffer[String] ={
    //形成开始start到预测predicted的日期
    var dateArrayBuffer=new ArrayBuffer[String]()
    val dateFormat= new SimpleDateFormat("yyyyMM");
    val cal1 = Calendar.getInstance()
    val cal2 = Calendar.getInstance()

    //设置训练数据中开始和结束日期
    cal1.set(startTime.substring(0,4).toInt,startTime.substring(4).toInt,0)
    cal2.set(endTime.substring(0,4).toInt,endTime.substring(4).toInt,0)

    //开始日期和预测日期的月份差
    val monthDiff = (cal2.getTime.getYear() - cal1.getTime.getYear()) * 12 +( cal2.getTime.getMonth() - cal1.getTime.getMonth())+predictedN
    var iMonth=0
    while(iMonth<=monthDiff){
      //日期加1个月
      cal1.add(Calendar.MONTH, iMonth)
      //保存日期
      dateArrayBuffer+=dateFormat.format(cal1.getTime)
      cal1.set(startTime.substring(0,4).toInt,startTime.substring(4).toInt,0)
      iMonth=iMonth+1
    }
    return dateArrayBuffer
  }

  /**
   * 批量生成日期（具体到月份的），用来保存
   * 格式为yyyy-MM
   * @param predictedN
   * @param startTime
   * @param endTime
   * @return
   */
  def productStartDayPredictDateRail(predictedN:Int,startTime:String,endTime:String): ArrayBuffer[String] ={
    //形成开始start到预测predicted的日期
    var dateArrayBuffer=new ArrayBuffer[String]()
    val dateFormat= new SimpleDateFormat("yyyy-MM");
    val cal1 = Calendar.getInstance()
    val cal2 = Calendar.getInstance()

    //设置训练数据中开始和结束日期
    cal1.set(startTime.substring(0,4).toInt,startTime.substring(5).toInt,0)
    cal2.set(endTime.substring(0,4).toInt,endTime.substring(5).toInt,0)

    //开始日期和预测日期的月份差
    val monthDiff = (cal2.getTime.getYear() - cal1.getTime.getYear()) * 12 +( cal2.getTime.getMonth() - cal1.getTime.getMonth())+predictedN
    var iMonth=0
    while(iMonth<=monthDiff){
      //日期加1个月
      cal1.add(Calendar.MONTH, iMonth)
      //保存日期
      dateArrayBuffer+=dateFormat.format(cal1.getTime)
      cal1.set(startTime.substring(0,4).toInt,startTime.substring(5).toInt,0)
      iMonth=iMonth+1
    }
    return dateArrayBuffer
  }

  /**
   * 批量生成日期（具体到日的），用来保存
   * 日期格式为：yyyyMMdd
   * @param predictedN
   * @param startTime
   * @param endTime
   */
  private def productStartDayPredictDay(predictedN:Int,startTime:String,endTime:String): ArrayBuffer[String] ={
    //形成开始start到预测predicted的日期
    var dayArrayBuffer=new ArrayBuffer[String]()
    val dateFormat= new SimpleDateFormat("yyyyMMdd");
    val cal1 = Calendar.getInstance()
    val cal2 = Calendar.getInstance()

    //设置训练数据中开始和结束日期
    cal1.set(startTime.substring(0,4).toInt,startTime.substring(4,6).toInt-1,startTime.substring(6).toInt)
    cal2.set(endTime.substring(0,4).toInt,endTime.substring(4,6).toInt-1,endTime.substring(6).toInt)

    //开始日期和预测日期的月份差
    val dayDiff = (cal2.getTimeInMillis-cal1.getTimeInMillis)/ (1000 * 60 * 60 * 24)+predictedN
    var iDay=0
    while(iDay<=dayDiff){
      //日期加1天
      cal1.add(Calendar.DATE, iDay)
      //保存日期
      dayArrayBuffer+=dateFormat.format(cal1.getTime)
      cal1.set(startTime.substring(0,4).toInt,startTime.substring(4,6).toInt-1,startTime.substring(6).toInt)
      iDay=iDay+1
    }

    return dayArrayBuffer
  }

  /**
   * 批量生成日期（具体到日的），用来保存
   * 日期格式为：yyyy-MM-dd
   * @param predictedN
   * @param startTime
   * @param endTime
   */
  private def productStartDayPredictDayRail(predictedN:Int,startTime:String,endTime:String): ArrayBuffer[String] ={
    //形成开始start到预测predicted的日期
    var dayArrayBuffer=new ArrayBuffer[String]()
    val dateFormat= new SimpleDateFormat("yyyy-MM-dd");
    val cal1 = Calendar.getInstance()
    val cal2 = Calendar.getInstance()

    //设置训练数据中开始和结束日期
    cal1.set(startTime.substring(0,4).toInt,startTime.substring(5,7).toInt-1,startTime.substring(8).toInt)
    cal2.set(endTime.substring(0,4).toInt,endTime.substring(5,7).toInt-1,endTime.substring(8).toInt)

    //开始日期和预测日期的月份差
    val dayDiff = (cal2.getTimeInMillis-cal1.getTimeInMillis)/ (1000 * 60 * 60 * 24)+predictedN
    var iDay=0
    while(iDay<=dayDiff){
      //日期加1天
      cal1.add(Calendar.DATE, iDay)
      //保存日期
      dayArrayBuffer+=dateFormat.format(cal1.getTime)
      cal1.set(startTime.substring(0,4).toInt,startTime.substring(5,7).toInt-1,startTime.substring(8).toInt)
      iDay=iDay+1
    }

    return dayArrayBuffer
  }

  /**
   * 把信息存储到hive中
   * 处理的数据格式多一个key列
   * @param dateDataRdd   合并了日期和数据的RDD
   * @param hiveColumnName
   * @param spark
   */
  private def keySaveInHive(dateDataRdd:RDD[Row],hiveColumnName:List[String],spark:SparkSession): Unit ={
    //把dateData转换成dataframe
    val schemaString=hiveColumnName(0)+" "+hiveColumnName(1)
    val schema=StructType(schemaString.split(" ")
      .map(fieldName=>StructField(fieldName,StringType,true)))
    val dateDataDf=spark.createDataFrame(dateDataRdd,schema)

    //dateDataDf存进hive中
    dateDataDf.write.mode(SaveMode.Overwrite).saveAsTable(outputTableName)
  }

  /**
   * 合并实际值和预测值，并加上日期,形成dataframe(Date,Data)
   * 并保存在hive中。
   * 处理的数据格式多一个key列
   * @param trainTsrdd      从hive中读取的数据
   * @param forecastValue   预测出来的数据（分为arima和holtwinters预测的）
   * @param predictedN      预测多少个值
   * @param startTime       开始日期
   * @param endTime         结束日期
   * @param sc
   * @param hiveColumnName  选择的列名字
   * @param keyName 选择哪个key输出
   * @param spark
   */
  def actualForcastDateKeySaveInHive(trainTsrdd:TimeSeriesRDD[String],forecastValue:RDD[(String,Vector)],predictedN:Int,startTime:String,endTime:String,sc:SparkContext,hiveColumnName:List[String],keyName:String,spark: SparkSession): Unit ={
    //在真实值后面追加预测值

   val foreacast= forecastValue.map{ case (key,forecastValu)=>(key,forecastValu.toArray.mkString(","))}

    val actualAndForcastRdd=trainTsrdd.map{
      _ match {
        case (key,actualValue)=>(key,actualValue.toArray.mkString(","))
      }
    }.join(foreacast)



    //获取日期，并转换成rdd
    var dateArray:ArrayBuffer[String]=new ArrayBuffer[String]()
    if(startTime.length==6){
      dateArray=productStartDatePredictDate(predictedN,startTime,endTime)
    }else if(startTime.length==7){
      dateArray=productStartDayPredictDateRail(predictedN,startTime,endTime)
    }else if(startTime.length==8){
      dateArray=productStartDayPredictDay(predictedN,startTime,endTime)
    }else if(startTime.length==10){
      dateArray=productStartDayPredictDayRail(predictedN,startTime,endTime)
    }
    val dateRdd=sc.parallelize(dateArray.toArray.mkString(",").split(",").map(date=>(date)))

    //合并日期和数据值,形成RDD[Row]+keyName
    val actualAndForcastArray=actualAndForcastRdd.collect()
    for(i<-0 until actualAndForcastArray.length){
      val dateDataRdd=actualAndForcastArray(i) match {
        case (key,value)=>{
          //指定key输出
          if(keyName==key){
            val actualAndForcast=sc.parallelize(value.toString().split(",").map(data=>(numChoose(data))))
            dateRdd.zip(actualAndForcast).map{
              _ match {
                case (date,data)=>Row(date,data)
              }
            }
          }else{
            sc.parallelize(Seq(Row("1")))
          }
        }
      }
      //保存信息
      if(dateDataRdd.collect()(0).toString()!="[1]"){
        keySaveInHive(dateDataRdd,hiveColumnName,spark)
      }
    }
  }
}
