package com.kuka.util;

/**
 * Created by kuai.jingjing on 2018/7/19.
 */
public class Test {
    public static final String topic = "my-replicated-topic";
    public static void main(String[] args){
        new KafkaProducer("kafka-producer-thread", topic).start();
        new KafkaConsumer("kafka-consumer-thread", topic).start();
    }

}
