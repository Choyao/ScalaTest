package com.choyao.apitest.processtest

import com.choyao.apitest.sourcetest.Sensor
import org.apache.flink.api.common.functions.RichFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.socketTextStream("192.168.10.151", 7777)
    val sensorStream = inputStream
      .map(line => {
        val arr = line.split(",")
        Sensor(arr(0).toString, arr(1).toLong, arr(2).toDouble)
      })
      .keyBy(_.id)
        .process(new MySideOutputStream())




    env.execute("side output test")
  }

}

case class MySideOutputStream() extends KeyedProcessFunction[String,Sensor,String]{
  override def processElement(value: Sensor, ctx: KeyedProcessFunction[String, Sensor, String]#Context, out: Collector[String]): Unit = {
    if(value.temperature >20)    ctx.output[Sensor](value)
  }
}
