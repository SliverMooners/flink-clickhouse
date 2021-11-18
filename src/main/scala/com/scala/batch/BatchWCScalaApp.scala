package com.scala.batch

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * 统计单词出现的次数
 */
object BatchWCScalaApp {

  def main(args: Array[String]): Unit = {

    val input = "file:///Users/yiche/fanc/flink/src/main/resources/hello.txt"

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(input)

    // 引入隐式转换
    import org.apache.flink.api.scala._

    text.flatMap(_.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1).print()
    // env.execute("BatchWCScalaApp")
  }

}
