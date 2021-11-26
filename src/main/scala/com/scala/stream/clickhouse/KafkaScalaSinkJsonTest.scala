package com.scala.stream.clickhouse

import com.scala.stream.clickhouse.domain.ScalaPerson
import com.scala.stream.clickhouse.sink.CustomSinkToClickHouse
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.LoggerFactory

import java.util.Properties

/**
 * @author fanc
 * @date 2021/11/17 3:39 下午
 * @Description ${kafka 接入demo}
 */
object KafkaScalaSinkJsonTest {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 非常关键，一定要设置启动检查点！！
    env.enableCheckpointing(10000)
    //配置kafka信息
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("zookeeper.connect", "localhost:2181")
    props.setProperty("group.id", "test-consumer-group")
    //读取数据
    val consumer = new FlinkKafkaConsumer[String]("chart", new SimpleStringSchema(), props)
    //设置只读取最新数据
    consumer.setStartFromLatest()
    val personStream = env.addSource(consumer).map(
      mapJson => {
        ScalaPerson(mapJson)
      }
    )
    personStream.addSink(new CustomSinkToClickHouse)
    env.execute("KafkaScalaSinkTest")
  }
}
