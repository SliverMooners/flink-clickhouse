package com.scala.batch.clickhouse

import com.flink.java.batch.FileWordCount
import com.scala.batch.clickhouse.sink.MyClickHouseOutPutFormat
import com.scala.stream.clickhouse.domain.ScalaPerson
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.slf4j.LoggerFactory

/**
 * 统计单词出现的次数
 */
object BatchFileClickHouseScalaApp {

  val logger = LoggerFactory.getLogger(BatchFileClickHouseScalaApp.getClass)

  def main(args: Array[String]): Unit = {


    val input = "file:///data/flinkProject/hello.txt"

    // val filePath = classOf[FileWordCount].getClassLoader.getResource("hello.txt").getPath

    logger.info("fanc test path:{}",input)


    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(input)

    // 引入隐式转换
    import org.apache.flink.api.scala._

    val personStream: DataSet[ScalaPerson] = text.map(new MapFunction[String, ScalaPerson] {
      override def map(line: String): ScalaPerson = {
        logger.info("fanc test BatchFileClickHouseScalaApp line:{}", line)
        val spilt = line.split(",")
        ScalaPerson(Integer.parseInt(spilt(0)), spilt(1), Integer.parseInt(spilt(2)))
      }
    })

    personStream.output(new MyClickHouseOutPutFormat)

    env.execute("DataStreamSinkToClickHouseApp")
  }

}
