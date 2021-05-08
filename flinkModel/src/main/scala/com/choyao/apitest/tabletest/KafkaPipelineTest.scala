package com.choyao.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

object KafkaPipelineTest {

  def main(args: Array[String]): Unit = {
    //1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val set = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env,set)

    //2.从Kafka读取数据

    tableEnv.connect(new Kafka()
      .version("0.10")
      .topic("sensor")
      .property("zookeeper.connect", "192.168.10.151:2181")
      .property("bootstrap.servers", "192.168.10.151:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")


    // 3.查询转换
    // 3.1.1 使用table api
    val sensorTable = tableEnv.from("kafkaInputTable")
    val resultTable = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    // 3.1.2 聚合转换
    val aggTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)

    // 3.2.1 SQL
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id,temperature from kafkaInputTable
        |where id ='sensor_1'
        """.stripMargin)


    // 3.2.2 SQL 聚合
    val aggSqlTable = tableEnv.sqlQuery(
      """
        |select COUNT(id) as ct from kafkaInputTable
        |group by id
        """.stripMargin)

    // 4.输出到kafka

    tableEnv.connect(new Kafka()
      .version("0.10")
      .topic("sinkTest")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv)
      .withSchema(new Schema()
          .field("id",DataTypes.STRING())
          .field("temperature",DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")
    resultSqlTable.insertInto("kafkaOutputTable")
    resultTable.insertInto("kafkaOutputTable")
    tableEnv.toRetractStream(aggSqlTable,Row.class).print("")

    tableEnv.execute("kafkaPipelineTest")
  }

}
