package com.choyao.apitest.sinktest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream = env.readTextFile(RedisSinkTest.getClass.getClassLoader.getResource("") + "../../src/main/resources/sensor.txt")

    val tranStream = inputStream.flatMap(_.split(",")).map((_,1)).keyBy(0).sum(1)

    // redis 单例连接配置
    val conf = new FlinkJedisPoolConfig.Builder().setPort(7281).setHost("7281").build()

    tranStream.addSink(new RedisSink[Tuple2[String,Int]](conf,new MyRedisMapper))


    env.execute("redis skin test")
  }

}
// redismapper
class MyRedisMapper extends RedisMapper[Tuple2[String,Int]]{
  override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET,"wordCount")

  override def getKeyFromData(t: Tuple2[String,Int]): String = t._1.toString

  override def getValueFromData(t: Tuple2[String,Int]): String = t._2.toString
}