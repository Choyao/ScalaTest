package com.choyao.apitest

import org.apache.commons.io.IOUtils
import org.apache.flink.streaming.api.scala._

object TransformTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val lineStream = env.readTextFile("C:\\Project\\ScalaTest\\flinkModel\\src\\main\\resources\\sensor.txt")


    val sensorStream = lineStream.map(data=> {
      val list = data.split(",")
      Sensor(list(0),list(1).toLong,list(2).toDouble)

    })
    sensorStream.keyBy("id")
      .minBy("temperature")
      .print()

    env.execute("Transform Test")

  }
}
