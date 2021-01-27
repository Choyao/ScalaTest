package com.choyao.apitest.statetest

import com.choyao.apitest.sourcetest.Sensor
import org.apache.flink.streaming.api.scala._

object StateTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inptStream = env.socketTextStream("192.168.10.151", 7777)
    val SensorStream = inptStream
      .flatMap(_.split(","))
      .map(line => new Sensor(line(0).toString,line(1).toLong,line(2).toDouble))





    env.execute("state test")

  }

}
