package com.haizhi;

import com.haizhi.kafka.KafkaClientConsumer;
import com.haizhi.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by youfeng on 2017/8/3.
 * 任务管理器入口文件
 */
public class TaskScheduler {

    private static final Logger logger = LoggerFactory.getLogger(TaskScheduler.class);


    static {
        //先装载配置信息
        PropertyUtil.loadProperties("application.properties");
    }


    public static void main(String... args) {
        logger.info("任务管理器开始执行...");

        //开始消费数据
        new KafkaClientConsumer().runTask();

//        try {
//            Thread.sleep(1000000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        logger.info("任务管理器运行结束...");
    }
}
