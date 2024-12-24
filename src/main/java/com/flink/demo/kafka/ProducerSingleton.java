package com.flink.demo.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;

/**
 * @author liwj
 * @date 2024/3/26 12:13
 * @description:
 */
public class ProducerSingleton {
    private static Producer producer;

    private ProducerSingleton() {
    }

    public static Producer getProducer(String clusterIp, String user, String password) {
        if (producer == null) {
            synchronized (ProducerSingleton.class) {
                if (producer == null) {
                    producer = initProducer(clusterIp, user, password);
                }
            }
        }
        return producer;
    }

    private static Producer initProducer(String clusterIp, String user, String password) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterIp);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + user + "\" password=\"" + password + "\";");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        return producer;
    }
}
