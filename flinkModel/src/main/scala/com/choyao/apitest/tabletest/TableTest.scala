package com.choyao.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}


object TableTest {
  def main(args: Array[String]): Unit = {
    //    // 流处理 用blink 方式
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val set = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, set)


    /*
    flink  读取 本地文件
     */
    /*    tableEnv.connect(new FileSystem().path(TableTest.getClass.getClassLoader.getResource("") + "../../src/main/resources/sensor.txt"))
          .withFormat(new OldCsv)
          .withSchema(new Schema().field("id", DataTypes.STRING())
            .field("tmp", DataTypes.BIGINT())
            .field("temp", DataTypes.DOUBLE()))
          .createTemporaryTable("sensor")

        val table = tableEnv.sqlQuery("select * from sensor")
        tableEnv.toRetractStream[(String, Long, Double)](table).print()
        */

    /*
    flink  读取kafka
     */
    tableEnv.connect(new Kafka()
      .version("0.10")
      .property("zookeeper.connect", "192.168.10.151:2181")
      .property("bootstrap.servers", "192.168.10.151:9092")
      .topic("sensor")
    ).withFormat(new Csv)
      .withSchema(new Schema().field("id", DataTypes.STRING())
        .field("tmp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE()))
      .createTemporaryTable("sensor")


    val table = tableEnv.from("sensor")
    tableEnv.toRetractStream[(String, Long, Double)](table).print()

    tableEnv.execute("table test")

  }

}
