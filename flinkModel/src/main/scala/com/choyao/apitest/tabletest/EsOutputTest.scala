package com.choyao.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, FileSystem, Json, Schema}

object EsOutputTest {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val set = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()
    val tableEnv = StreamTableEnvironment.create(env,set)

    // 2. 创建链接
    tableEnv.connect(new FileSystem().path(EsOutputTest.getClass.getClassLoader.getResource("") +"../../src/main/resources/sensor.txt"))
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE())
      )
        .createTemporaryTable("fileInputTable")

    // 3. 转换操作
    val resultTable = tableEnv.sqlQuery(
      """
        |select id,sum(temperature) as tem from fileInputTable where temperature >=10.0
        |group by id
        """.stripMargin)


    // 4. 写到ES中
    tableEnv.connect(new Elasticsearch()
        .version("6")
        .host("localhost",9200,"http")
        .index("sensor")
        .documentType("temperature")
       )
      .inUpsertMode()
      .withFormat(new Json())
      .withSchema(new Schema()
          .field("id",DataTypes.STRING())
          .field("tem",DataTypes.DOUBLE())
      )
      .createTemporaryTable("esOutputTable")

    resultTable.insertInto("esOutputTable")


    tableEnv.execute("es output test")

  }

}
