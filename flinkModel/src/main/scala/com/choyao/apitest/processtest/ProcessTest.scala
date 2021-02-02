package com.choyao.apitest.processtest

import com.choyao.apitest.sourcetest.Sensor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object ProcessTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.socketTextStream("192.168.10.151", 7777)
    val pendingStream = inputStream
      .map(line =>  Sensor(line.split(",")(0).toString, line.split(",")(1).toLong, line.split(",")(2).toDouble))
      .keyBy(_.id)
      .process(new WarningKeyedProcessFunction())
      .print()


    env.execute("process test")

  }

}


class MyKeyedProcessFunction() extends KeyedProcessFunction[String, Sensor, String] {
  override def processElement(value: Sensor, ctx: KeyedProcessFunction[String, Sensor, String]#Context, out: Collector[String]): Unit = {

    // 测输出流
    ctx.output[Sensor](new OutputTag("hot"),value)

    // 当前水印
    ctx.timerService().currentWatermark()

    // 定时器
    ctx.timerService().registerEventTimeTimer(10000)

  }
}

// 连续10s 连续温度上升 就报警
class WarningKeyedProcessFunction() extends KeyedProcessFunction[String, Sensor, String] {

  lazy val valueState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor("temperature", classOf[Double]))

  lazy val timerTimestamp: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTimestamp", classOf[Long]))

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, String ]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(ctx.getCurrentKey + timerTimestamp.value() + "temper" +valueState.value())
    ctx.timerService().deleteProcessingTimeTimer(timerTimestamp.value())
    timerTimestamp.clear()

  }

  override def processElement(value: Sensor, ctx: KeyedProcessFunction[String, Sensor, String]#Context, out: Collector[String]): Unit = {

    val lastTemp = valueState.value()
    val time = timerTimestamp.value()
    valueState.update(value.temperature)
    //判断是否是第一条数据 设置定时器
    if (lastTemp < value.temperature && time == 0) {
      val ts = ctx.timerService().currentProcessingTime() + 10000L
      ctx.timerService().registerProcessingTimeTimer(ts)
      timerTimestamp.update(ts)
    } else if (lastTemp > value.temperature) { //关闭定时器 时间初始化
      ctx.timerService().deleteProcessingTimeTimer(time)
      timerTimestamp.clear()
    }


  }
}
