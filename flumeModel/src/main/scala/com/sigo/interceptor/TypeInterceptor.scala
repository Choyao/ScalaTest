package com.sjgo.intercptor

import java.util

import org.apache.flume.{Context, Event}
import org.apache.flume.interceptor.Interceptor
import scala.jdk.CollectionConverters._

class TypeInterceptor extends Interceptor {



  var eventLists: util.ArrayList[Event] = new util.ArrayList[Event]()

  // 初始化
  override def initialize(): Unit = {

  }

  /**
   *
   * @param event
   * @return Event
   *         处理单个事件
   */
  override def intercept(event: Event): Event = {
    // 获取事件头
    val headers = event.getHeaders()

    // 获取事件body内容
    val body = event.getBody().toString()

    // 根据body中事件名添加头信息
    if (body.contains("hello")) {
      headers.put("event", "hello")
    } orElse {
      headers.put("event", "no")
    }
    event


  }

  // 批量处理事件环节
  override def intercept(list: util.List[Event]): util.List[Event] = {

    eventLists.clear()


    for(event <- list.asScala){
      eventLists.add(intercept(event))
    }

    eventLists
  }

  // 关闭
  override def close(): Unit = ???

  class Bulider extends Interceptor.Builder {
    override def build(): Interceptor = new TypeInterceptor()

    override def configure(context: Context): Unit = ???
  }

}
