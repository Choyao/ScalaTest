package com.choyao.apitest.sinktest

import com.choyao.apitest.sourcetest.Sensor
import org.apache.flink.streaming.api.scala._

object SinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.readTextFile(SinkTest.getClass.getClassLoader.getResource("") + "../../src/main/resources/sensor.txt")


    val outputStream = inputStream.map(data => {
      val l = data.split(",")
      Sensor(l(0),l(1).toLong,l(2).toDouble)
    })
    outputStream.writeAsCsv(SinkTest.getClass.getClassLoader.getResource("") + "../../src/main/resources/sensor.csv")


    env.execute("sink test")
  }
}
