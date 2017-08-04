package com.haizhi.kafka;

import com.haizhi.util.PropertyUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by youfeng on 2017/8/4.
 * kafka消费者
 */
public class KafkaClientConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaClientConsumer.class);

    private final KafkaConsumer<String, String> consumer;

    private final List<String> topics;
    private long pullTimeout;

    public KafkaClientConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", PropertyUtil.getProperty("kafka.servers"));
        props.put("group.id", PropertyUtil.getProperty("kafka.group.id"));
        props.put("key.deserializer", PropertyUtil.getProperty("key.deserializer"));
        props.put("value.deserializer", PropertyUtil.getProperty("value.deserializer"));
        props.put("enable.auto.commit", "false");

        String inputTopic = PropertyUtil.getProperty("kafka.topics");

        this.topics = Arrays.asList(inputTopic.split(","));
        this.pullTimeout = Long.valueOf(PropertyUtil.getProperty("poll.timeout"));
        consumer = new KafkaConsumer<>(props);

        logger.info("初始化kafka完成...");
    }

    public void runTask() {
        logger.info("开始消费数据...");
        try {
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(pullTimeout);
                if (records.count() > 0) {
                    logger.info("kafka 中读取的数据数目为: count = {}", records.count());
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("{}:{}", record.key(), record.value());
                    }
                }
                consumer.commitSync();
            }
        } catch (Exception e) {
            logger.error("消费kafka数据失败...", e);
        } finally {
            this.consumer.commitSync();
            this.consumer.close();
        }

    }
}
