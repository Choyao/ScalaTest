package com.choyao.apitest.tabletest

import org.apache.commons.io.IOUtils
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object FileOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val envSet = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env,envSet)

    tableEnv.connect(new FileSystem().path(FileOutputTest.getClass.getClassLoader.getResource("") + "../../src/main/resources/sensor.txt"))
      .withFormat(new Csv)
      .withSchema(new Schema().field("id", DataTypes.STRING())
        .field("time", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE()))
      .createTemporaryTable("sensor")

    val resTable = tableEnv.sqlQuery("select id,count(id) from sensor group by id")


    tableEnv.toRetractStream[(String,Long)](resTable).print()

    tableEnv.execute("file table test")
  }
}
