package com.scala.stream.clickhouse.domain

import com.alibaba.fastjson.JSON

/**
 * @author fanc
 * @date 2021/11/17 10:10 上午
 * @Description ${todo}
 */
case class ScalaPerson(id: Int, name: String, age: Int)


object ScalaPerson {
  def apply(json: String): ScalaPerson = {
    val jsonObject = JSON.parseObject(json)
    ScalaPerson(
      jsonObject.getInteger("id"),
      jsonObject.getString("name"),
      jsonObject.getInteger("age")
    )
  }
}

