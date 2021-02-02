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

    // 自定义状态编程
    val alertStream = sensorStream
      .keyBy(_.id)
        .map(new WarningMapper())
        .print()

    // keyed state 有状态的 算子
    val alertStream2 = sensorStream
      .keyBy(_.id)
//      .flatMapWithState()
        .mapWithState[(String,Double,Double),Double] {
          case (sensor: Sensor,None) => (null,Some(sensor.temperature))
          case (sensor: Sensor,lastTemp:Some[Double]) =>{
            if( (sensor.temperature - lastTemp.get).abs > 10)
              (("warning",sensor.temperature,lastTemp.get),Some(sensor.temperature))
            else
              (null,Some(sensor.temperature))
          }

        }




    env.execute("state test")



  }

}
// 状态 编程 keyed 实现 Rich函数
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



