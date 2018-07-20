package com.kuka.util;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by kuai.jingjing on 2018/7/19.
 */
public class KafkaConsumer extends Thread{

    private final ConsumerConnector consumerConnector;
    private final String topic;

    public KafkaConsumer(String name, String topic) {
        super(name);

        Properties properties = new Properties();
        properties.put("zookeeper.connect", "101.37.146.66:2181");// 声明zookeeper
        properties.put("group.id", "groupConsumer1");// 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
        properties.put("zookeeper.session.timeout.ms", "40000");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "1000");
        consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
        this.topic = topic;
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

        topicCountMap.put(topic, 1); // 一次从主题中获取一个数据

        Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumerConnector.createMessageStreams(topicCountMap);

        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据

        ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();

        while(iterator.hasNext()){

            String message = new String(iterator.next().message());

            System.out.println("接收到: " + message);

        }
    }
}
