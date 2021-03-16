package com.choyao.apitest.processtest

import com.choyao.apitest.sourcetest.Sensor
import org.apache.flink.api.common.functions.RichFunction
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 状态后端  保存在RocksDB 中  还有
    env.setStateBackend(new RocksDBStateBackend(" "))

    // 状态后端 状态保存在内存中
    env.setStateBackend(new MemoryStateBackend())

    // 状态保存在 远程文件系统中
    env.setStateBackend(new FsStateBackend(""))


    val inputStream = env.socketTextStream("192.168.10.151", 7777)
    val sensorStream = inputStream
      .map(line => {
        val arr = line.split(",")
        Sensor(arr(0).toString, arr(1).toLong, arr(2).toDouble)
      })

    val hotStream = sensorStream
      .keyBy(_.id)
        .process(new MySideOutputStream)

    val lowStream = hotStream.getSideOutput[Sensor](OutputTag("lower"))

    hotStream.print("high")
    lowStream.print("low")

    env.execute("side output test")
  }

}

case class MySideOutputStream() extends KeyedProcessFunction[String, Sensor, Sensor] {
  override def processElement(value: Sensor, ctx: KeyedProcessFunction[String, Sensor, Sensor]#Context, out: Collector[Sensor]): Unit = {
    if (value.temperature < 20) ctx.output[Sensor](OutputTag("lower"), value) else out.collect(value)
  }
}
