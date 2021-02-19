package com.choyao.apitest.tabletest

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

object TableTest {
  def main(args: Array[String]): Unit = {
//    // 流处理 用blink 方式
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val set = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env,set)


    tableEnv.connect(new FileSystem().path(TableTest.getClass.getClassLoader.getResource("") + "../../src/main/resources/sensor.txt"))
      .withFormat(new OldCsv)
      .withSchema(new Schema().field("id",DataTypes.STRING())
        .field("tmp",DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE()))
      .createTemporaryTable("sensor")

    val table:Table = tableEnv.sqlQuery("select * from sensor")

    val rest = tableEnv.toRetractStream[(String,Long,Double)](table).print()

    tableEnv.execute("table test")

  }

}
