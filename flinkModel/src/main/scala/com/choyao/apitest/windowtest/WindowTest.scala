package com.choyao.apitest.windowtest

import org.apache.flink.shaded.curator.org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream = env.readTextFile(WindowTest.getClass.getClassLoader.getResource("") + "../../src/main/resources/sensor.txt")

    //keyed 数据
    val tupStream = inputStream.flatMap(_.split(",")).map((_, 1))

    // 有序的 事件时间 水印
    tupStream.assignAscendingTimestamps(_._1.toLong)

    // 周期性 无序 watermark 延迟 30毫秒
    tupStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Int)](Time.milliseconds(30)) {
      override def extractTimestamp(element: (String, Int)): Long =element._1.toLong
    })

    // 间隔性 watermark
    tupStream.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(String, Int)] {
      override def checkAndGetNextWatermark(lastElement: (String, Int), extractedTimestamp: Long): Watermark = ???

      override def extractTimestamp(element: (String, Int), previousElementTimestamp: Long): Long = ???
    })



    tupStream.keyBy(_._1)
      //keyed window TumbingEventTimeWindows
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))

    tupStream.keyBy(_._1)
      //keyed window TumbingProssingTimeWindows
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))

    tupStream.keyBy(_._1)
      //keyed window SlidingEventTimeWindows
      .window(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(3)))

    tupStream.keyBy(_._1)
      //keyed window SlidingProcessingTimeWindows
      .window(SlidingProcessingTimeWindows.of(Time.seconds(2), Time.seconds(3)))

  }
}
