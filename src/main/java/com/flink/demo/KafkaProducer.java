package com.flink.demo;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * This class provides methods for producing messages to Kafka topics.
 */
public class KafkaProducer {

    /**
     * Sends a message to a Kafka topic.
     *
     * @param topic   the name of the Kafka topic
     * @param key     the key for the message
     * @param message the message to be sent
     */
    public static void sendMessage(String topic, String key, String message) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "172.30.141.125:9092,172.30.141.126:9092,172.30.141.133:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);

        // Send the message
        try {
            producer.send(new ProducerRecord<>(topic, key, message));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Close the producer
            producer.close();
        }
    }
}
