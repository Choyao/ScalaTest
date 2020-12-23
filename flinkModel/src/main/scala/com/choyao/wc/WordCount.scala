package com.choyao.wc
import org.apache.flink.api.scala._
object WordCount {
  def main(args: Array[String]): Unit = {
    //批处理环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val inputPath ="C:\\Project\\ScalaTest\\flinkModel\\src\\main\\resources\\hello.txt"

    val inputDataSet = env.readTextFile(inputPath)

    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()




      }

}
