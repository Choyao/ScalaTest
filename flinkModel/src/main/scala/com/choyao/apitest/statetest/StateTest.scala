package com.choyao.apitest.statetest

import com.choyao.apitest.sourcetest.Sensor
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RichFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object StateTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inptStream = env.socketTextStream("192.168.10.151", 7777)
    val sensorStream = inptStream.map(line =>{ val arr = line.split(",")
      new Sensor(arr(0),arr(1).toLong,arr(2).toDouble)} )
    val alertStream = sensorStream
      .keyBy(_.id)
        .map(new WarningMapper())
        .print()
    env.execute("state test")

  }

}
// 状态 编程 keyed
class WarningMapper() extends RichMapFunction[Sensor,String]{
  var valueState:ValueState[Double] = _
  override def open(configuration: Configuration): Unit = {
   valueState =  getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState",classOf[Double],30.0))
  }


  override def map(in: Sensor ): String  = {
//    val thatTemp = if (valueState.value() ==0.0) 30.0 else valueState.value()
    val thatTemp = valueState.value()
    valueState.update(in.temperature)

   if((in.temperature- thatTemp).abs > 10.0 ) "waring" + in.id else "normal" + in.id
  }
}



