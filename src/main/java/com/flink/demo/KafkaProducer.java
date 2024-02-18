package com.flink.demo;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducer {


    public static void sendMessage(String topic,String key,String message){
        Properties properties = new Properties();
        properties.put("bootstrap.servers","172.30.141.125:9092,172.30.141.126:9092,172.30.141.133:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);

        // 发送消息
        try {
            producer.send(new ProducerRecord<String, String>(topic, key, message));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭生产者实例
            producer.close();
        }
    }
}
