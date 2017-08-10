package com.haizhi;

import com.haizhi.kafka.KafkaClientConsumer;
import com.haizhi.manage.TaskManage;
import com.haizhi.util.PropertyUtil;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by youfeng on 2017/8/3.
 * 任务管理器入口文件
 */
public class TaskScheduler implements Callable<Void> {

    private static final Logger logger = LoggerFactory.getLogger(TaskScheduler.class);


    static {
        //先装载配置信息
        PropertyUtil.loadProperties("application.properties");
    }

    //引擎句柄
    private KieSession kSession;

    public TaskScheduler(String topic, String groupId, int maxKafkaNum) {
        // 这里要加锁
        KieServices ks = KieServices.Factory.get();
        KieContainer kContainer = ks.getKieClasspathContainer();
        kSession = kContainer.newKieSession("session-task");

        kSession.setGlobal("kafkaClient",
                new KafkaClientConsumer(topic, groupId));

        //kSession.setGlobal("logger", LoggerFactory.getLogger(TaskScheduler.class));
        kSession.insert(new TaskManage(topic, maxKafkaNum));
    }

    public static void main(String... args) {

        int availProcessors = Runtime.getRuntime().availableProcessors();
        logger.info("当前CPU数目: {}", availProcessors);

        logger.info("任务管理器开始执行...");

        String topics = PropertyUtil.getProperty("kafka.topics");
        String maxKafkaNums = PropertyUtil.getProperty("kafka.max.consume.num");
        String groupIds = PropertyUtil.getProperty("kafka.group.id");
        List<String> topicList = Arrays.asList(topics.split(","));
        List<String> maxKafkaList = Arrays.asList(maxKafkaNums.split(","));
        List<String> groupIdList = Arrays.asList(groupIds.split(","));

        logger.info("当前启动的线程数目: {}", topicList.size());
        ExecutorService pool = Executors.newFixedThreadPool(topicList.size());
        for (int index = 0; index < topicList.size(); index++) {
            pool.submit(new TaskScheduler(topicList.get(index),
                    groupIdList.get(index),
                    Integer.valueOf(maxKafkaList.get(index))));
        }

        pool.shutdown();
        try {
            logger.info("开始等待所有线程完成....");
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.error("ERROR", e);
        }

        logger.info("任务管理器运行结束...");
    }

    @Override
    public Void call() throws Exception {

        kSession.fireUntilHalt();
        return null;
    }
}
