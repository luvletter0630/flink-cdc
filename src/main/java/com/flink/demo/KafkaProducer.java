package com.flink.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * This class provides methods for producing messages to Kafka topics.
 */
@Slf4j
public class KafkaProducer {

    /**
     * Sends a message to a Kafka topic.
     *
     * @param topic   the name of the Kafka topic
     * @param key     the key for the message
     * @param message the message to be sent
     */

    public static void sendMessage(String clusterIp, String topic, String key, String message) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterIp);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"admin-succ\";");
        props.put("acks", "all");
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        // Send the message
        try {
            RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, message)).get();
            log.info("消息已成功发送至主题：{} , 分区： {}", metadata.topic(), metadata.partition());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Close the producer
            producer.close();
        }
    }

    public static void main(String[] args) {
        KafkaProducer.sendMessage("172.30.145.213:9092", "Test-Topic", "1", "1213");
    }
}
