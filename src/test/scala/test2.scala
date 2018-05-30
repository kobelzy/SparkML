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
    println(time)
    time.toLocalDateTime.getMonthValue
    time.toLocalDateTime.getDayOfMonth
    time.toLocalDateTime.getYear
  }
}
