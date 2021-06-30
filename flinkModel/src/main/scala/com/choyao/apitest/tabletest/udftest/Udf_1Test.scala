package com.choyao.apitest.tabletest.udftest

import com.choyao.apitest.tabletest.FileOutputTest
import org.apache.commons.io.IOUtils
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, Schema}

case class Seneor(id:String  ,timestamp:Long,temperature :Double)
object Udf_1Test {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val setting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env,setting)

    val  tableStram =   tableEnv.connect(new FileSystem().path(Udf_1Test.getClass.getClassLoader.getResource("") + "../../src/main/resources/sensor.txt"))
      .withSchema(new Schema())


  }
}
