package com.scala.stream.clickhouse

import com.scala.stream.clickhouse.sink.ClickHouseJDBCSinkScala
import com.yiche.recommend.bigdata.flink.sink.clickhouse.{ClickHouseJDBCSinkScala, CustomSinkToClickHouse}
import com.yiche.recommend.bigdata.flink.sink.clickhouse.domain.RecommendInfo
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.LoggerFactory

import java.util.Properties


object FlinkSyncDataClickHouseBatch {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    println("---------------start-----------------------")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置kafka配置信息
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("zookeeper.connect", "localhost:2181")
    props.setProperty("group.id", "test-consumer-group")


    //
    val consumer = new FlinkKafkaConsumer[String]("chart", new SimpleStringSchema, props)
    val stream = env.addSource(consumer)


    // 处理数据格式
    val log_convert_message = stream.map(row => ParserMap.parserStr(row)).filter(rowMap => rowMap != null)

    println(log_convert_message)

    // 创建clickhouseSink连接
    val clickhouseSink = new ClickHouseJDBCSinkScala()

    log_convert_message.addSink(clickhouseSink)
    env.execute("Kafka Window WordCount")

  }
}
