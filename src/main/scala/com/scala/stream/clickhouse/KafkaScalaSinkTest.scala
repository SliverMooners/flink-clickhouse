package com.scala.stream.clickhouse

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.flink.streaming.api.scala._

/**
 * @author fanc
 * @date 2021/11/17 3:39 下午
 * @Description ${kafka 接入demo}
 */
object KafkaScalaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "ke02:9092,ke03:9092,ke04:9092")
    properties.setProperty("group.id", "flink-kafka-001")
    properties.setProperty("key.deserializer", classOf[StringSerializer].getName)
    properties.setProperty("value.deserializer", classOf[StringSerializer].getName)
    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink-kafka", new SimpleStringSchema(), properties))
    stream.print()
    env.execute()
  }
}
