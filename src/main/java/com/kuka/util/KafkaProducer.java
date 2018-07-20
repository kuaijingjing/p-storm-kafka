package com.kuka.util;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;

/**
 * Created by kuai.jingjing on 2018/7/19.
 */
public class KafkaProducer extends Thread{

    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties properties = new Properties();

    public KafkaProducer(String name, String topic) {
        super(name);

        properties.put("zookeeper.connect", "101.37.146.66:2181");// 声明zookeeper
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list", "101.37.146.66:9092,101.37.146.66:9093");// 声明kafka

        producer = new kafka.javaapi.producer.Producer<>(new ProducerConfig(properties));
        this.topic = topic;
    }

    @Override
    public void run() {
        int i = 0;
        while (true) {
            try {
                String message = "java_test_message" + i;
                producer.send(new KeyedMessage<Integer, String>(topic, message));
                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            i ++;
        }

    }


}
