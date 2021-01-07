package com.choyao.apitest.sinktest

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.readTextFile(EsSinkTest.getClass.getClassLoader.getResource("") + "../../src/main/resources/hello.txt")
    val wordCountStream = inputStream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("192.168.10.151",9200))
    wordCountStream.addSink(new ElasticsearchSink.Builder[(String, Int)](httpHosts,new ElasticsearchSinkFunction[Tuple2[String,Int]](){
      override def process(t: (String, Int), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val dataMap = new util.HashMap[String,String]()
        dataMap.put("world",t._1)
        dataMap.put("count",t._2.toString)

        val indexRequst = Requests.indexRequest().`type`("reading").index("wordcount").source(dataMap)
        requestIndexer.add(indexRequst)

      }
    }).build())


    env.execute("es sink test")
  }
}

