import java.sql.Timestamp
import java.util.Calendar

/**
  * Created by Administrator on 2018/5/30.
  */
object test2 {
  def main(args: Array[String]): Unit = {
    println(getTime())
  }
  def getTime()={
    val time=Timestamp.valueOf("2018-01-11 11:11:11")
    val time2=Timestamp.valueOf("2018-01-14 11:11:11")
    println(time.compareTo(time2))
    println((time2.getTime-time.getTime)/(60*60*24*1000))
    println(time)
    time.toLocalDateTime.getMonthValue
    time.toLocalDateTime.getDayOfMonth
    time.toLocalDateTime.getYear
  }
}
