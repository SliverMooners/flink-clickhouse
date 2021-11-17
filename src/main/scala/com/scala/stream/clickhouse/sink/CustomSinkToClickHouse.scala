package com.scala.stream.clickhouse.sink

import com.scala.stream.clickhouse.domain.ScalaPerson
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.sql.{Connection, DriverManager, PreparedStatement}


/**
 * 存入clickhouse
 */
class CustomSinkToClickHouse extends RichSinkFunction[ScalaPerson] {

  private[stream] var connection: Connection = _
  private[stream] var ps: PreparedStatement = _


  /**
    * 获取数据库连接
    */
  def getConnection() = {
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
    DriverManager.getConnection("jdbc:clickhouse://10.24.19.31:8123")
  }

  /**
    * 在open方法中建立connection
    *
    * @param parameters
    * @throws Exception
    */
  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connection = getConnection()
    val sql = "INSERT INTO fanc.user_table (id, name, age) VALUES (?,?,?)"
    ps = connection.prepareStatement(sql)
    System.out.println("open")
  }


  /**
    * 每条记录插入时调用一次
    *
    * @param value
    * @param context
    * @throws Exception
    */
  @throws[Exception]
  override def invoke(value: ScalaPerson, context: SinkFunction.Context[_]): Unit = {
    System.out.println("invoke~~~~~~~~~")
    // 未前面的占位符赋值
    ps.setInt(1, value.id)
    ps.setString(2, value.name)
    ps.setInt(3, value.age)
    ps.executeUpdate
  }

  /**
    * 在close方法中要释放资源
    *
    * @throws Exception
    */
  @throws[Exception]
  override def close(): Unit = {
    super.close()
    if (ps != null) ps.close()
    if (connection != null) connection.close()
  }
}
