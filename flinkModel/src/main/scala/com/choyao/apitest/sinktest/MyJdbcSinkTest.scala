package com.choyao.apitest.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.choyao.apitest.sourcetest.Sensor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object MyJdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.readTextFile(EsSinkTest.getClass.getClassLoader.getResource("") + "../../src/main/resources/hello.txt")
    val wordCountStream = inputStream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)


    wordCountStream.addSink(new MyJdbcSinkFunction())


    env.execute()
  }
}

class MyJdbcSinkFunction() extends RichSinkFunction[Tuple2[String,Int]] {

  var con: Connection = _
  var insertPre: PreparedStatement = _
  var updataPre: PreparedStatement = _

  override def open(configuration: Configuration): Unit = {
    con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123")
    insertPre = con.prepareStatement("insert  into  sensor (id,temp) values (?,?)")
    updataPre = con.prepareStatement("update sensor set id = ? where id = ?")

  }

  override def close(): Unit = {

    con.close()
    insertPre.close()
    updataPre.close()
  }

  override def invoke(value: Tuple2[String,Int], context: SinkFunction.Context[_]): Unit = {
    updataPre.setString(1, value._1)
    updataPre.setDouble(2, value._2)
    updataPre.execute()
    if (updataPre.getUpdateCount == 0) {
      insertPre.setString(1, value._1)
      insertPre.setDouble(2, value._2)
      insertPre.execute()
    }

  }
}
