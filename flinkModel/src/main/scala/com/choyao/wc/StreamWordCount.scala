package com.choyao.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val parameter = ParameterTool.fromArgs(args)
    val host=parameter.get("host")
    val port = parameter.getInt("port")
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val wordCount = senv.socketTextStream(host,port)
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()
        .setParallelism(1)

    senv.execute("streaming word count")

  }

}
