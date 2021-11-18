package com.scala.stream.clickhouse

import com.scala.stream.clickhouse.domain.ScalaPerson
import com.scala.stream.clickhouse.sink.CustomSinkToClickHouse
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

/**
 * @author fanc
 * @date 2021/11/17 10:11 上午
 * @Description ${clickhouse 存入数据demo}
 */
object ClickHouseScalaSinkTest {

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(this.getClass)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 引入隐式转换
    import org.apache.flink.api.scala._

    val text = env.socketTextStream("localhost", 9999)
    val personStream = text.map(new MapFunction[String, ScalaPerson] {
      override def map(value: String): ScalaPerson = {
        logger.info("fanc test ClickHouseScalaSinkTest value :{}",value)
        val spilt = value.split(",")

        ScalaPerson(Integer.parseInt(spilt(0)), spilt(1), Integer.parseInt(spilt(2)))
      }
    })
    personStream.addSink(new CustomSinkToClickHouse)

    env.execute("DataStreamSinkToClickHouseApp")
  }
}
