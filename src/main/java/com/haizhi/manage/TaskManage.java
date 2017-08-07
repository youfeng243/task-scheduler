package com.haizhi.manage;

import com.haizhi.hbase.HBaseDao;
import com.haizhi.util.PropertyUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by youfeng on 2017/8/4.
 * 任务管理器
 */
public class TaskManage {

    private static final Logger logger = LoggerFactory.getLogger(TaskManage.class);

    //family
    private static final String COLUMN_FAMILY = "data";

    //kafka数据消费阈值
    public static final int MAX_KAFKA_NUM = 10;

    //kafka实际消费阈值
    private int maxKafkaNum;

    // kafka中统计的数据数目
    private int kafkaCount = 0;

    // 全量区
    private String wholeSpace;

    // 增量区
    private String increaseSpace;

    private HBaseDao hBaseDao;

    //mongodb业务表名
    private String tableName;

    //hbase表信息
    private Table wholeTable;
    private Table increaseTable;

    public TaskManage(String tableName, int maxKafkaNum) {
        wholeSpace = PropertyUtil.getProperty("whole.change.flag");
        increaseSpace = PropertyUtil.getProperty("increase.change.flag");

        logger.info("当前需要流转数据的表: {}", tableName);
        logger.info("当前kafka消费阈值: {}", maxKafkaNum);

        this.tableName = tableName;
        this.maxKafkaNum = maxKafkaNum;

        String quorum = PropertyUtil.getProperty("hbase.zookeeper.quorum");
        String clientPort = PropertyUtil.getProperty("hbase.zookeeper.property.clientPort");
        String master = PropertyUtil.getProperty("hbase.master");
        hBaseDao = new HBaseDao(quorum, clientPort, master);
        createHBaseNameSpace(wholeSpace.split(":")[0]);
        createHBaseNameSpace(increaseSpace.split(":")[0]);

        try {
            hBaseDao.createTable(tableName, new String[]{COLUMN_FAMILY});

            String wholeTableName = wholeSpace + tableName;
            wholeTable = hBaseDao.getTable(wholeTableName);

            String increaseTableName = increaseSpace + tableName;
            increaseTable = hBaseDao.getTable(increaseTableName);

        } catch (Exception e) {
            logger.error("创建表信息: ", e);
        }

    }

    public int getMaxKafkaNum() {
        return maxKafkaNum;
    }

    public void setMaxKafkaNum(int maxKafkaNum) {
        this.maxKafkaNum = maxKafkaNum;
    }

    public int getKafkaCount() {
        return kafkaCount;
    }

    public void setKafkaCount(int kafkaCount) {
        this.kafkaCount = kafkaCount;
    }

    //增加表数据数目
    public void incKafkaCount(int num) {
        kafkaCount += num;
    }

    //减少表数据数目
    public void clearKafkaCount() {
        kafkaCount = 0;
    }

    //创建HBase命名空间
    private void createHBaseNameSpace(String hBaseNameSpace) {
        try {
            hBaseDao.createNamespace(hBaseNameSpace);
        } catch (IOException e) {
            logger.error("命名空间创建失败: {}", hBaseNameSpace);
            logger.error("ERROR:", e);
        }
    }

    //添加数据到增量区
    public void addIncData(String rowkey, String value) {
        try {
            hBaseDao.addRow(increaseTable, rowkey, COLUMN_FAMILY, tableName, value);
            logger.debug("增量区: {} {} {}", tableName, rowkey, value);
        } catch (Exception e) {
            logger.error("写入数据异常:", e);
        }
    }

    //添加数据到全量区
    public void addWholeData(String rowkey, String value) {
        try {
            hBaseDao.addRow(wholeTable, rowkey, COLUMN_FAMILY, tableName, value);
            logger.debug("全量区: {} {} {}", tableName, rowkey, value);
        } catch (Exception e) {
            logger.error("写入数据异常:", e);
        }
    }

    //从增量区把数据存入全量区
    public void transfer() {

        if (kafkaCount < MAX_KAFKA_NUM) {
            logger.error("数据量没有达到，不需要清洗...");
            return;
        }

        logger.info("开始转移数据到全量区...table = {}", tableName);

        //这里转移数据
        try {
            ResultScanner resultScanner = hBaseDao.getAllRows(increaseTable);
            if (resultScanner == null) {
                logger.error("ResultScanner获取失败..");
                return;
            }

            for (Result result : resultScanner) {
                for (Cell cell : result.rawCells()) {
                    String rowkey = new String(CellUtil.cloneRow(cell));
                    String value = new String(CellUtil.cloneValue(cell));

                    // 把数据插入全量区
                    addWholeData(rowkey, value);

                    //从增量区删除数据
                    hBaseDao.delRow(increaseTable, rowkey);
                }
            }
        } catch (Exception e) {
            logger.error("转移数据异常:", e);
        }

        //清空状态
        clearKafkaCount();

        logger.info("数据从增量区转移到全量区完成...table = {}", tableName);
    }
}
