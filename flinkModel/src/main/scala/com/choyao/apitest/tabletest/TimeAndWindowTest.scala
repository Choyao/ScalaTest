package com.choyao.apitest.tabletest

import com.choyao.apitest.processtest.WarningKeyedProcessFunction
import com.choyao.apitest.sourcetest.Sensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TimeAndWindowTest {

  def main(args: Array[String]): Unit = {
    // 获得流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置时间特性
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val setting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    // 基于流处理环境得到 tableEnv
    val tableEnv = StreamTableEnvironment.create(env,setting)


    val inputStream = env.readTextFile(TimeAndWindowTest.getClass.getClassLoader.getResource("") + "../../src/main/resources/sensor.txt")
    val pendingStream = inputStream
      .map(line => {
        val data = line.split(",")
        Sensor(data(0).toString, data(1).toLong, data(2).toDouble)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(1)) {
      override def extractTimestamp(t: Sensor): Long = t.timestamp * 1000
    })

    val sensorTable = tableEnv.fromDataStream(pendingStream,'id,'temperature,'timestamp.rowtime as 'et)
    tableEnv.createTemporaryView("sensorTable",sensorTable)
   val resultTable = tableEnv.sqlQuery(
     """
       |select id ,count(id),avg(temperature),tumble_end(et,interval '10' second)
       |from  sensorTable
       |group by
       |id , tumble(et,interval '10' second)
       |""".stripMargin)

     resultTable.toRetractStream[Row].print()

    //    table.printSchema()
//    table.toAppendStream[Row].print()

    env.execute("time and window table")
  }
}
