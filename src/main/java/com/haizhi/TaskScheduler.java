package com.haizhi;

import com.haizhi.manage.TaskManage;
import com.haizhi.util.PropertyUtil;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
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

        KieServices ks = KieServices.Factory.get();
        KieContainer kContainer = ks.getKieClasspathContainer();
        KieSession kSession = kContainer.newKieSession("session-task");

        kSession.insert(new TaskManage(kSession));
        kSession.fireUntilHalt();
        logger.info("任务管理器运行结束...");
    }
}
