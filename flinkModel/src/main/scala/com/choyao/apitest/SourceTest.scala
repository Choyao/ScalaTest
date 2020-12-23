package com.choyao.apitest

import org.apache.flink.streaming.api.scala._


case class Sensor(id:String,timestamp:Long,temperature:Double)
object SourceTest {
  def main(args: Array[String]): Unit = {

    val stream1 = StreamExecutionEnvironment.getExecutionEnvironment


    val dataList = List(
      Sensor("sensor_1",1111111115,32.2)
      ,Sensor("sensor_2",1111111111,5.5)
      ,Sensor("sensor_3",1111111119,30.2)
      ,Sensor("sensor_4",1111111110,12.5)
    )
    //从集合中读取流

    stream1.fromCollection(dataList).print()
    //从文件中读取流
    val inputPath = "C:\\Users\\ADMIN\\IdeaProjects\\ScalaTest\\flinkModel\\src\\main\\resources\\sensor.txt"
    val fileSensor = stream1.readTextFile(inputPath)

    fileSensor.print()

    stream1.execute("dataList ")


  }
}
