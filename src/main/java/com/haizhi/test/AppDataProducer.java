package com.haizhi.test;

import com.haizhi.mongo.Mongo;
import com.haizhi.util.PropertyUtil;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by youfeng on 2017/8/4.
 * 工商表数据测试
 */
public class AppDataProducer implements Callable<Void> {

    private static final Logger logger = LoggerFactory.getLogger(AppDataProducer.class);


    static {
        //先装载配置信息
        PropertyUtil.loadProperties("application.properties");
    }

    //数据业务表mongodb句柄
    private Mongo appDataMongo;

    // 数据库
    private MongoDatabase appDataDatabase;

    // 数据库表名称
    private String tableName;

    public AppDataProducer(String tableName) {

        this.tableName = tableName;

        //初始化数据表句柄
        appDataMongo = new Mongo(PropertyUtil.getProperty("data.mongo.host"),
                PropertyUtil.getProperty("data.mongo.username"),
                PropertyUtil.getProperty("data.mongo.password"),
                PropertyUtil.getProperty("data.mongo.auth.db"));

        //任务表数据库
        appDataDatabase = appDataMongo.getDb(PropertyUtil.getProperty("data.mongo.database"));
    }

    @Override
    public Void call() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", PropertyUtil.getProperty("kafka.servers"));
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", PropertyUtil.getProperty("key.serializer"));
        props.put("value.serializer", PropertyUtil.getProperty("value.serializer"));

        String topic = PropertyUtil.getProperty("kafka.topics");
        Producer<String, String> producer = new KafkaProducer<>(props);

        MongoCursor<Document> cursor = appDataDatabase.getCollection(tableName).find().iterator();
        while (cursor.hasNext()) {

            Document document = cursor.next();

            String _record_id = document.getString("_record_id");
            String key = tableName + "#" + _record_id;

            logger.info("{} : {}", key, document);

            producer.send(new ProducerRecord<>(topic, key, document.toJson()));
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                logger.error("休眠异常:", e);
            }
        }

        cursor.close();
        return null;
    }

    public static void main(String... args) {

        ExecutorService threadPool = Executors.newFixedThreadPool(2);

        threadPool.submit(new AppDataProducer("enterprise_data_gov"));
        threadPool.submit(new AppDataProducer("annual_reports"));

        logger.info("线程已经加载完成，等待结束...");
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
            logger.info("线程池结束正常..");
        } catch (InterruptedException e) {
            logger.error("线程被中断: ", e);
        }
    }

}
