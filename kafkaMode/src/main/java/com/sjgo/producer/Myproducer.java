package com.sjgo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 简单的kafka生产者类
 */
public class Myproducer  {


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.10.152:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String topic = "first";
        KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10; i++) {
            String value = "sjgo+" + i;
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,"sigo", value);
            myProducer.send(record);
        }
        myProducer.close();
    }
}
