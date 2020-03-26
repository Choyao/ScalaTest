package com.sjgo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 简单的kafka消费者类
 */
public class MyConsumer {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.10.152:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdate");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //订阅主题
        consumer.subscribe(Collections.singletonList("first"));

        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(1));

        for (ConsumerRecord<String, String> record : poll) {
            System.out.println(record.key() +"---"+record.value());
        }
    }
    }
}
