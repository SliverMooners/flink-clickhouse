package com.scala.batch.clickhouse.sink

import com.scala.stream.clickhouse.domain.ScalaPerson
import org.apache.flink.api.common.io.{OutputFormat, RichOutputFormat}
import org.apache.flink.configuration.Configuration
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * @author fanc
 * @date 2021/11/17 5:37 下午
 * @Description ${todo}
 */

class MyClickHouseOutPutFormat extends RichOutputFormat[ScalaPerson]{

  val logger = LoggerFactory.getLogger(this.getClass)


  private[batch] var connection: Connection = _
  private[batch] var ps: PreparedStatement = _

  override def configure(parameters: Configuration): Unit = {
  }

  /**
   * 获取数据库连接
   */
  def getConnection() = {
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
    DriverManager.getConnection("jdbc:clickhouse://10.24.19.31:8123")
  }

  //写数据
  override def writeRecord(value: ScalaPerson): Unit = {
    logger.info("fanc test MyClickHouseOutPutFormat value:{}",value)
    // 未前面的占位符赋值
    ps.setInt(1, value.id)
    ps.setString(2, value.name)
    ps.setInt(3, value.age)
    ps.executeUpdate
  }

  override def close(): Unit = {
    if (null != connection){
      connection.close()
    }
  }

  //初始化数据库连接
  override def open(taskNumber: Int, numTasks: Int): Unit = {
    connection = getConnection()
    val sql = "INSERT INTO fanc.user_table (id, name, age) VALUES (?,?,?)"
    ps = connection.prepareStatement(sql)
    logger.info("open")
  }
}