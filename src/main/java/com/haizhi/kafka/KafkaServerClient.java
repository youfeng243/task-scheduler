package com.haizhi.kafka;

import com.haizhi.util.PropertyUtil;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by youfeng on 2017/8/10.
 * kafka消费者
 */
public class KafkaServerClient {

    private final long pullTimeout;

    private final KafkaConsumer<String, String> consumer;

    public KafkaServerClient(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", PropertyUtil.getProperty("kafka.servers"));
        props.put("group.id", topic + UUID.randomUUID().toString());
        props.put("session.timeout.ms", "30000");
        //props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", PropertyUtil.getProperty("key.deserializer"));
        props.put("value.deserializer", PropertyUtil.getProperty("value.deserializer"));
        props.put("enable.auto.commit", "false");

        this.pullTimeout = Long.valueOf(PropertyUtil.getProperty("poll.timeout"));
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public ConsumerRecords<String, String> consumerData() {
        ConsumerRecords<String, String> records = consumer.poll(pullTimeout);
        consumer.commitSync();
        return records;
    }
}
