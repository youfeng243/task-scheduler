package com.haizhi.manage;

import com.haizhi.hbase.HBaseDao;
import com.haizhi.kafka.KafkaServerClient;
import com.haizhi.kafka.KafkaServerProducer;
import com.haizhi.util.JsonUtil;
import com.haizhi.util.PropertyUtil;
import javafx.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by youfeng on 2017/8/4.
 * 任务管理器
 */
public class TaskManage {

    private static final Logger logger = LoggerFactory.getLogger(TaskManage.class);

    //实时消息
    private static final String REAL_TIME_MSG = "event_msg";

    //数据消息
    private static final String DATA_MSG = "data_msg";

    private KafkaServerClient kafkaClient;

    //hbase句柄
    private HBaseDao hBaseDao;

    //规则引擎句柄
    private KieSession kSession;

    //实时处理通道topic
    private String realTimeTopic;

    //kafka生产者
    private KafkaServerProducer kafkaProducer;

    //运行对象
    private Map<String, Pair<FactHandle, DataTask>> factMap = new HashMap<>();

    public TaskManage(KieSession kSession) {

        String quorum = PropertyUtil.getProperty("hbase.zookeeper.quorum");
        String clientPort = PropertyUtil.getProperty("hbase.zookeeper.property.clientPort");
        String master = PropertyUtil.getProperty("hbase.master");
        hBaseDao = new HBaseDao(quorum, clientPort, master);

        //kafka订阅的总的数据总线
        String topic = PropertyUtil.getProperty("kafka.data.topic");
        realTimeTopic = PropertyUtil.getProperty("kafka.realtime.topic");

        //kafka消费者
        kafkaClient = new KafkaServerClient(topic);

        //kafka生产者
        kafkaProducer = new KafkaServerProducer();

        this.kSession = kSession;
    }

    // 消息转发
    private void msgTransmit(String msg) {
        //json转map
        Map<String, String> dataMap = JsonUtil.jsonToObject(msg, Map.class);
        if (dataMap == null) {
            logger.error("当前数据转换json失败: {}", msg);
            return;
        }
        String topic = dataMap.get("head");
        String value = dataMap.get("content");
        String key = dataMap.get("_record_id");

        if (topic == null || value == null || key == null) {
            logger.error("数据格式错误: {}", msg);
            return;
        }

        //判断是否已经添加过数据
        Pair<FactHandle, DataTask> factHandleDataTaskPair = factMap.get(topic);
        if (factHandleDataTaskPair == null) {
            DataTask dataTask = new DataTask(topic, hBaseDao);
            FactHandle factHandle = kSession.insert(dataTask);
            factHandleDataTaskPair = new Pair<>(factHandle, dataTask);

            factMap.put(topic, factHandleDataTaskPair);
            logger.info("添加新的消息处理对象: {}", topic);
        }

        //转发消息
        kafkaProducer.send(topic, key, value);
        //logger.info("当前发送消息: {} {} {}", topic, key, value);
        //kSession.update(factHandleDataTaskPair.getKey(), factHandleDataTaskPair.getValue());
    }

    public void consumerData() {

        //消费kafka数据
        ConsumerRecords<String, String> records = kafkaClient.consumerData();
        for (ConsumerRecord<String, String> record : records) {
            String key = record.key();
            String value = record.value();

            //实时消息
            if (Objects.equals(key, REAL_TIME_MSG)) {
                kafkaProducer.send(realTimeTopic, key, value);
                //logger.info("转发实时消息: {} : {}", key, value);
                continue;
            }

            //数据消息
            if (Objects.equals(key, DATA_MSG)) {
                msgTransmit(value);
                continue;
            }

            logger.error("无法处理的未知消息: key = {} value = {}", key, value);
        }
        //logger.info("消费kafka数据...");
    }

    public void update() {
        for (Map.Entry<String, Pair<FactHandle, DataTask>> entry: factMap.entrySet()) {
            kSession.update(entry.getValue().getKey(), entry.getValue().getValue());
        }
        logger.info("更新状态完成....");
    }
}
