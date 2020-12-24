package com.choyao.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.util.Random


case class Sensor(id:String,timestamp:Long,temperature:Double)
object SourceTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //从集合中读取流
    val dataList = List(
      Sensor("sensor_1",1111111115,32.2)
      ,Sensor("sensor_2",1111111111,5.5)
      ,Sensor("sensor_3",1111111119,30.2)
      ,Sensor("sensor_4",1111111110,12.5)
    )
    val listSoucrce = env.fromCollection(dataList)

    //从文件中读取流
    val inputPath = "C:\\Users\\ADMIN\\IdeaProjects\\ScalaTest\\flinkModel\\src\\main\\resources\\sensor.txt"
    val fileSensor = env.readTextFile(inputPath)

    //从kafka中读取流
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.10.151:9092")
    properties.setProperty("group.id","consumer-group")
    val kafkaSource = env.addSource(new FlinkKafkaConsumer010[String]("sensor",new SimpleStringSchema(),properties))
    //kafkaSource.print()

    val mySource = env.addSource(new MySensor())
//    mySource.print().setParallelism(1)

    print(Random.nextGaussian())

    env.execute("dataList ")
  }

  class MySensor extends SourceFunction[Sensor]{
    var flag:Boolean = true
    override def cancel(): Unit =  flag= false

    override def run(sourceContext: SourceFunction.SourceContext[Sensor]): Unit = {
      // 初始化 temp
      var temp = 1.to(10).map(num => ("sensor_" + num,Random.nextDouble() * 100))

      while(flag) {

        //每次细微变化
        temp = temp.map(data => (data._1 , data._2 + Random.nextGaussian()) )

        //逐条发送
        var curTime = System.currentTimeMillis()
        temp.foreach(data => sourceContext.collect(Sensor(data._1,curTime,data._2)))

        Thread.sleep(1000)
      }
    }
  }

}
