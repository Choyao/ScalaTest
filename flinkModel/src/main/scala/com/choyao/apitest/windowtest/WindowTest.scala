package com.choyao.apitest.windowtest

import org.apache.flink.shaded.curator.org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream = env.readTextFile(WindowTest.getClass.getClassLoader.getResource("") + "../../src/main/resources/sensor.txt")

    //keyed 数据
    val tupStream = inputStream.flatMap(_.split(",")).map((_, 1))


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
