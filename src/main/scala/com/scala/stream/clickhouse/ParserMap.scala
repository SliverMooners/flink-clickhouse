package com.scala.stream.clickhouse

import com.alibaba.fastjson.JSON

import scala.collection.JavaConverters._

object ParserMap {

  def parserStr(json: String): java.util.Map[String, Any] = {
    var user: Map[String, Any] = Map()
    val jObject = JSON.parseObject(json)
    jObject.keySet().asScala.foreach(key => {
      user += (key -> jObject.getString(key))
    })
    println(user)
    user.asJava
  }
}
