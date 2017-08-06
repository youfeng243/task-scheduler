package com.haizhi.manage;

import com.haizhi.hbase.HBaseDao;
import com.haizhi.util.PropertyUtil;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by youfeng on 2017/8/4.
 * 任务管理器
 */
public class TaskManage {

    private static final Logger logger = LoggerFactory.getLogger(TaskManage.class);

    //family
    private static final String COLUMN_FAMILY = "data";

    //kafka数据消费阈值
    private static final int MAX_KAFKA_NUM = 100;

    // kafka中统计的数据数目
    //private int kafkaCount = 0;

    // 不同表中增量数目
    private Map<String, Integer> incNumMap = new HashMap<>();

    //已经存在的表信息
    private Map<String, Boolean> tableIsExist = new HashMap<>();

    // 全量区
    private String wholeSpace;

    // 增量区
    private String increaseSpace;

    private HBaseDao hBaseDao;

    public TaskManage() {
        wholeSpace = PropertyUtil.getProperty("whole.change.flag");
        increaseSpace = PropertyUtil.getProperty("increase.change.flag");


        String quorum = PropertyUtil.getProperty("hbase.zookeeper.quorum");
        String clientPort = PropertyUtil.getProperty("hbase.zookeeper.property.clientPort");
        String master = PropertyUtil.getProperty("hbase.master");
        hBaseDao = new HBaseDao(quorum, clientPort, master);
        createHBaseNameSpace(wholeSpace.split(":")[0]);
        createHBaseNameSpace(increaseSpace.split(":")[0]);
    }

    //增加表数据数目
    public void incTableNum(String tableName, int num) {
        //incNumMap.merge(tableName, num, (a, b) -> b + a);

        if (incNumMap.containsKey(tableName)) {
            Integer current = incNumMap.get(tableName);
            incNumMap.put(tableName, current + num);
        } else {
            incNumMap.put(tableName, num);
        }
    }

    //减少表数据数目
    public void clearTableNum(String tableName) {
        incNumMap.put(tableName, 0);
    }

    //创建HBase命名空间
    private void createHBaseNameSpace(String hBaseNameSpace) {
        //先创建HBase 命名空间
        // 获取HBase的命名空间
        try {

            hBaseDao.createNamespace(hBaseNameSpace);
        } catch (IOException e) {
            logger.error("命名空间创建失败: {}", hBaseNameSpace);
            logger.error("ERROR:", e);
        }
    }

    //创建表
    private void createTable(String tableName) {
        if (!tableIsExist.containsKey(tableName)) {
            try {
                hBaseDao.createTable(tableName, new String[]{COLUMN_FAMILY});
            } catch (Exception e) {
                logger.error("创建失败: {}", tableName);
                logger.error("ERROR", e);
            }
            tableIsExist.put(tableName, true);
        }
    }

    //添加数据到增量区
    public void addIncData(String rowkey, String key, String value) {
        String tableName = increaseSpace + key;
        createTable(tableName);
        try {
            Table table = hBaseDao.getTable(tableName);
            hBaseDao.addRow(table, rowkey, COLUMN_FAMILY, key, value);
            table.close();
            logger.info("添加数据成功: {} {} {}", rowkey, key, value);
        } catch (Exception e) {
            logger.error("写入数据异常:", e);
        }
    }

    //删除增量区数据
    public void deleteAllInc(String tableName) {
        String fullTableName = increaseSpace + tableName;

        try {
            hBaseDao.deleteTable(fullTableName);
        } catch (Exception e) {
            logger.error("删除表异常:", e);
        }
    }

    //获得达到阈值的列表信息
    public List<String> getTableList() {
        List<String> tableList = new ArrayList<>();

        for (Map.Entry<String, Integer> entry : incNumMap.entrySet()) {
            if (entry.getValue() >= MAX_KAFKA_NUM) {
                tableList.add(entry.getKey());
            }
        }

        return tableList;
    }
}
