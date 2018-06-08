import java.sql.Timestamp

import com.cloudera.sparkts.{DateTimeIndex, DayFrequency, UniformDateTimeIndex}
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}

/**
  * Auther: lzy
  * Description:
  * Date Created by： 19:03 on 2018/5/29
  * Modified By：
  */
object dateTimeTest {
    val format=new SimpleDateFormat("yyyy-MM-dd")
val formater=DateTimeFormatter.ofPattern("yyyy-MM-dd")
    def main(args: Array[String]): Unit = {
        val timestampe=Timestamp.valueOf("2018-05-1 00:00:00")
        println(timestampe.setTime(timestampe.getTime+1000*60*60*24*30))
        println(timestampe)
        val zoneId = ZoneId.systemDefault()

        val forcastTimeIndex:UniformDateTimeIndex=DateTimeIndex.uniformFromInterval(
            ZonedDateTime.of(2017, 5, 1, 0, 0, 0, 0,zoneId),
            ZonedDateTime.of(2017, 5, 31, 0, 0, 0, 0, zoneId),
            new DayFrequency(1))
//        val forcastTimeArr=forcastTimeIndex.toZonedDateTimeArray()
//                .map(date=>{
//                    date.format(formater)
//
//                })
//        forcastTimeArr.foreach(println)
    }
}
