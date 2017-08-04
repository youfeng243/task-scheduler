package com.haizhi.test;

import com.haizhi.util.PropertyUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by youfeng on 2017/8/4.
 * kafka生产者
 */
public class KafkaServerProducer {

    static {
        //先装载配置信息
        PropertyUtil.loadProperties("application.properties");
    }

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "cs6:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        Properties prop = new Properties();
//        prop.put("bootstrap.servers", PropertyUtil.getProperty("kafka.servers"));
//        prop.put("key.serializer", PropertyUtil.getProperty("key.serializer"));
//        prop.put("value.serializer", PropertyUtil.getProperty("value.serializer"));
//       // prop.put("auto.create.topics.enable", "true");
        String topic = PropertyUtil.getProperty("kafka.topics");
//
//
////        prop.put("bootstrap.servers", PropertyUtil.getProperty("kafka.servers"));
////        prop.put("key.serializer", PropertyUtil.getProperty("key.serializer"));
////        prop.put("value.serializer", PropertyUtil.getProperty("value.serializer"));
////        prop.put("acks", PropertyUtil.getProperty("kafka.acks"));
////        prop.put("retries", PropertyUtil.getInt("kafka.retries"));
////        prop.put("linger.ms", PropertyUtil.getInt("kafka.linger.ms"));
////        prop.put("auto.create.topics.enable", true);
//
        Producer<String, String> producer = new KafkaProducer<>(props);

        int count = 0;
        while (true) {
            producer.send(new ProducerRecord<>(topic, Integer.toString(count), Integer.toString(count)));
            count++;
            Thread.sleep(1000);
        }
    }


}
