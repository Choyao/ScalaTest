package com.sjgo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

public class InterceptorProducer {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.10.152:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.sjgo.interceptor.CountInterceptor");
        interceptors.add("com.sjgo.interceptor.TimeInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {

            ProducerRecord<String, String> record = new ProducerRecord<>("first", "sigo", "sjgo" + i);
            kafkaProducer.send(record);
        }

        kafkaProducer.close();
    }
}
