package org.lzy.streaming
import java.sql._

import org.apache.spark.sql.{ForeachWriter, Row}
class JDBCSink(url:String,user :String,pwd:String) extends ForeachWriter[Row]{
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

  override def process(value:Row): Unit = {
    statement.executeUpdate(s"insert into table(key,pv) values(${value.getString(0)},${value.getString(1)})")
  }

  override def close(errorOrNull: Throwable): Unit = {
    connection.close()
  }
}
