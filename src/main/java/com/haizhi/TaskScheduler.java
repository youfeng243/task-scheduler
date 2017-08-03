package com.haizhi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by youfeng on 2017/8/3.
 * 任务管理器入口文件
 */
public class TaskScheduler {

    private static final Logger logger = LoggerFactory.getLogger(TaskScheduler.class);

    public static void main(String... args) {
        logger.info("任务管理器开始执行...");

//        try {
//            Thread.sleep(1000000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        logger.info("任务管理器运行结束...");
    }
}
