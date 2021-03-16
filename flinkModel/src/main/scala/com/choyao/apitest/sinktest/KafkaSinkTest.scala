package com.choyao.apitest.sinktest

import java.util.Properties

import com.choyao.apitest.sourcetest.Sensor
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010,FlinkKafkaProducer09}
import org.apache.flink.util.PropertiesUtil


object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val inputStream = env.readTextFile(KafkaSinkTest.getClass.getClassLoader.getResource("") + "../../src/main/resources/sensor.txt")

    val propertis = new Properties()

    propertis.setProperty("bootstrap.servers","192.168.10.151:9092")
    propertis.setProperty("group.id","consumer-group")
    val inputStream = env.addSource(new FlinkKafkaConsumer010[String]("sensor",new SimpleStringSchema(),propertis))

    val sensorStream = inputStream.map(data => {
      val arr = data.split(",")
      Sensor(arr(0), arr(1).toLong, arr(2).toDouble).toString
    })

    sensorStream.addSink(
      new FlinkKafkaProducer09[String]("192.168.10.151:9092", "sinktest", new SimpleStringSchema())
    )

    env.execute("kafka sink test")
  }
}
