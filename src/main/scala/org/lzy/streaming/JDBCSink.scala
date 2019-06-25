package org.lzy.streaming
import java.sql._

import org.apache.spark.sql.ForeachWriter
class JDBCSink(url:String,user :String,pwd:String) extends ForeachWriter[(String,Long)]{
  val driver ="org.postgresql.Driver"
//  val url="jdbc:postgresql://localhost:5432/postgres"
  var connection:Connection=_
  var statement:Statement=_
  override def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection=DriverManager.getConnection(url,user,pwd)
    statement=connection.createStatement()
    true
  }

  override def process(value: (String, Long)): Unit = {
    statement.executeUpdate(s"insert into table(key,pv) values(${value._1},${value._2})")
  }

  override def close(errorOrNull: Throwable): Unit = {
    connection.close()
  }
}
