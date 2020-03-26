package com.sjgo;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 自定义拦截器
 */
public class TypeInterceptor implements Interceptor{

    private List<Event> events;

    //初始化
    public void initialize() {
        events = new ArrayList<Event>();
    }

    //拦截处理单个事件
    public Event intercept(Event event) {
        Map<String,String> headers = event.getHeaders();
        String body = new String(event.getBody());
        if (body.contains("hello")){
            headers.put("event","hello");
        }else {
            headers.put("event","no");
        }
        return event;
    }

    //处理批量事件
    public List<Event> intercept(List<Event> list) {

        events.clear();
        for (Event event : list) {
            events.add(intercept(event));
        }

        return events;
    }

    //关闭
    public void close() {

    }

    /**
     * 反射类构造拦截器
     */
    public static class Bulider implements Interceptor.Builder{

        public Interceptor build() {

            return new TypeInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
