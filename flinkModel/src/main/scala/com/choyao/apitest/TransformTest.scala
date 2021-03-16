package com.choyao.apitest

import org.apache.commons.io.IOUtils
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

object TransformTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val lineStream = env.readTextFile(TransformTest.getClass.getResource("/")+"../../src/main/resources/sensor.txt")

    val sensorStream = lineStream.map(data=> {
      val list = data.split(",")
      Sensor(list(0),list(1).toLong,list(2).toDouble)

    })
// 1.keyBy
/*    sensorStream.keyBy("id")
      .minBy("temperature")
      .print()*/

// 2.reduce
/*    sensorStream.keyBy("id")
        .reduce((curState,newData) =>{
          Sensor(curState.id,newData.timestamp,curState.temperature.min(newData.temperature))
        })
        .print()*/

// 3.split and select
      val splitStream = sensorStream.split(data => {
        if(data.temperature > 30.0) Seq("high") else Seq("low")
      })
      val highStream = splitStream.select("high")
      val lowStream = splitStream.select("low")

// 4.connect and coMap or coFlatMap
      val conStream = highStream.connect(lowStream)
      val conMapStream = conStream.map(highStream => {
        ("warning",highStream.id,highStream.temperature,highStream.timestamp)
      },lowStream => {
        ("normal",lowStream.temperature)
      })
      conMapStream.print()


    env.execute("Transform Test")

  }


  class MyReduceFunction extends  ReduceFunction[Sensor]{
    override def reduce(t: Sensor, t1: Sensor): Sensor = ???
  }
}
