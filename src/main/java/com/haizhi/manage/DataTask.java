package com.haizhi.manage;

import com.haizhi.hbase.HBaseDao;
import com.haizhi.kafka.KafkaServerClient;
import com.haizhi.util.PropertyUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by youfeng on 2017/8/4.
 * 数据批处理任务
 */
public class DataTask {

    private static final Logger logger = LoggerFactory.getLogger(DataTask.class);

    // kafka消费数据状态
    public static final int STATUS_CONSUMER = 0;

    //清洗状态
    public static final int STATUS_CLEAN = 1;

    public static final int STATUS_CONSUMERING = 2;

    //清理中
    public static final int STATUS_CLEANING = 3;

    //HBase 列族
    private static final String COLUMN_FAMILY = "data";

    // 当前最大消费数量
    private final int maxConsumerNum;

    // 当前以及消费的数目
    private int currentNum = 0;

    private Table hBaseIncTable;
    private Table hBaseFullTable;

    private String tableName;

    // 记录当前程序状态
    private int status;

    // hBase 句柄
    private HBaseDao hBaseDao;

    // 线程池
    private ExecutorService executorService = Executors.newFixedThreadPool(1);

    //kafka消费者
    private KafkaServerClient kafkaClient;

    public DataTask(String topic, HBaseDao hBaseDao) {

        //HBase 操作句柄
        this.hBaseDao = hBaseDao;
        tableName = topic;

        String wholeSpace = PropertyUtil.getProperty("whole.change.flag");
        String increaseSpace = PropertyUtil.getProperty("increase.change.flag");

        String hBaseIncTableName = increaseSpace + ":" + tableName;
        String hBaseFullTableName = wholeSpace + ":" + tableName;

        //创建命名空间
        createHBaseNameSpace(wholeSpace);
        createHBaseNameSpace(increaseSpace);

        //创建表
        createHBaseTable(hBaseIncTableName);
        createHBaseTable(hBaseFullTableName);

        // 获得表信息
        try {
            hBaseIncTable = hBaseDao.getTable(hBaseIncTableName);
            hBaseFullTable = hBaseDao.getTable(hBaseFullTableName);
        } catch (IOException e) {
            logger.error("获得hBase table 句柄失败:", e);
        }

        //初始化消费状态
        status = STATUS_CONSUMER;

        //从配置文件中读取最大kafka消费数量
        maxConsumerNum = PropertyUtil.getInt("max.consumer.num");

        //初始化kafka
        kafkaClient = new KafkaServerClient(topic);
        logger.info("初始化kafka完成...topic = {}", topic);
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    private void createHBaseTable(String hBaseTableName) {
        try {
            hBaseDao.createTable(hBaseTableName, new String[]{COLUMN_FAMILY});
        } catch (Exception e) {
            logger.error("创建表信息: {}", hBaseTableName);
            logger.error("创建表信息: ", e);
        }
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


    // 存储数据到增量区..
    public int consumerData() {

        status = STATUS_CONSUMERING;
        executorService.submit(new Runnable() {
            @Override
            public void run() {

                //消费kafka数据
                ConsumerRecords<String, String> records = kafkaClient.consumerData();
                int count = records.count();
                if (count <= 0) {
                    status = STATUS_CONSUMER;
                    return;
                }

                logger.info("开始消费数据: {} {}", tableName, count);

                List<Put> putList = new ArrayList<>();
                for (ConsumerRecord<String, String> record : records) {
                    String _record_id = record.key();
                    String docJson = record.value();

                    Put put = new Put(Bytes.toBytes(_record_id));
                    put.addColumn(Bytes.toBytes(COLUMN_FAMILY),
                            Bytes.toBytes(tableName),
                            Bytes.toBytes(docJson));
                    putList.add(put);
                }

                try {
                    hBaseIncTable.put(putList);
                } catch (IOException e) {
                    logger.error("存储HBase异常:", e);
                }

                currentNum += records.count();
                if (currentNum >= maxConsumerNum) {
                    // 如果消费数量达到一定程度，则进入清洗流程...
                    status = STATUS_CLEAN;
                    logger.info("{} 数据量超过 {} 进入清洗状态", tableName, currentNum);
                } else {
                    //如果没达到数据量则继续消费
                    status = STATUS_CONSUMER;
                }
            }
        });

        return status;
    }

    // 清洗流程
    public int cleanData() {

        logger.info("进入清洗状态: {}", tableName);
        status = STATUS_CLEANING;
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                do {

                    ResultScanner resultScanner = null;

                    try {
                        resultScanner = hBaseDao.getAllRows(hBaseIncTable);
                    } catch (Exception e) {
                        logger.error("读取增量区数据出错:");
                    }
                    if (resultScanner == null) {
                        logger.error("ResultScanner获取失败..");
                        break;
                    }

                    List<Put> putList = new ArrayList<>();
                    List<Delete> deleteList = new ArrayList<>();
                    for (Result result : resultScanner) {
                        for (Cell cell : result.rawCells()) {
                            String rowKey = new String(CellUtil.cloneRow(cell));
                            String value = new String(CellUtil.cloneValue(cell));
                            String key = new String(CellUtil.cloneQualifier(cell));

                            Put put = new Put(Bytes.toBytes(rowKey));
                            put.addColumn(Bytes.toBytes(COLUMN_FAMILY),
                                    Bytes.toBytes(key),
                                    Bytes.toBytes(value));
                            putList.add(put);

                            Delete delete = new Delete(Bytes.toBytes(rowKey));
                            deleteList.add(delete);
                        }
                    }

                    //插入全量区数据
                    try {
                        hBaseFullTable.put(putList);
                        hBaseIncTable.delete(deleteList);
                    } catch (IOException e) {
                        logger.error("数据转移失败:", e);
                    }

                    logger.info("{} 数据清洗完成...", tableName);
                } while (false);


                //清理状态
                currentNum = 0;
                status = STATUS_CONSUMER;

                logger.info("清理状态完成..topic = {} currentNum = {} status = {}",
                        tableName, currentNum, status);
            }
        });
        return status;
    }

    public static void main(String... args) {
        PropertyUtil.loadProperties("application.properties");
        KafkaServerClient kafkaClient = new KafkaServerClient("enterprise_data_gov");
        while (true) {
            ConsumerRecords<String, String> records = kafkaClient.consumerData();
            for (ConsumerRecord<String, String> record : records) {
                String _record_id = record.key();
                String docJson = record.value();
                System.out.println("测试消费端: + " + _record_id + " : " + docJson);
            }
        }

    }
}
