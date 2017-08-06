package com.haizhi.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
//import java.util.stream.Collectors;

public class HBaseDao {
    private static Logger logger = LoggerFactory.getLogger(HBaseDao.class);

    private Connection connection = null;

    private Admin admin = null;

    //初始化成功标志
    private boolean isInitSuccess;

    static {
        System.setProperty("hadoop.home.dir", "/");
    }

    public HBaseDao(String quorum, String clientPort, String master) {
        isInitSuccess = false;
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.property.clientPort", clientPort);
        conf.set("hbase.master", master);

        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            logger.error("连接HBASE失败..");
            logger.error("ERROR:", e);
            return;
        }
        isInitSuccess = true;
        logger.info("HBase 连接初始化成功..");
    }

    public boolean isInitSuccess() {
        return isInitSuccess;
    }

    //创建命名空间
    public void createNamespace(String nameSpace) throws IOException {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            logger.warn("命名空间已经存在, 不需要创建: {}", nameSpace);
        }
    }

    //判断表是否存在
    public boolean isTableExist(String tableName) throws IOException {
        if (!isInitSuccess) {
            logger.error("HBase未初始化...");
            return false;
        }
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public boolean isTableExist(Table table) throws IOException {
        if (!isInitSuccess) {
            logger.error("HBase未初始化...");
            return false;
        }
        return admin.tableExists(table.getName());
    }

    public synchronized Table getTable(String tableName) throws IOException {
        if (!isInitSuccess) {
            logger.error("HBase未初始化...");
            return null;
        }
        return connection.getTable(TableName.valueOf(tableName));
    }

    // 创建数据库表
    public void createTable(String tableName, String[] columnFamilies)
            throws Exception {

        if (!isInitSuccess) {
            logger.error("HBase未初始化...");
            return;
        }

        // 新建一个数据库管理员
        if (isTableExist(tableName)) {
            logger.info("表 " + tableName + " 已存在！");
            return;
        }

        // 新建一个students表的描述
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

        // 在描述里添加列族
        for (String columnFamily : columnFamilies) {
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
        }

        // 根据配置好的描述建表
        admin.createTable(tableDesc);
        logger.info("创建表 " + tableName + " 成功!");
    }

    public List<String> getTableNameList() {
        List<String> tableNameList = new ArrayList<>();
        if (!isInitSuccess) {
            logger.error("HBase未初始化...");
            return tableNameList;
        }

        try {
            TableName[] tableNames = admin.listTableNames();
            for (TableName tableName : tableNames) {
                tableNameList.add(tableName.getNameAsString());
                //logger.info("Table: " + tableName.getNameAsString());
            }
        } catch (IOException e) {
            logger.error("ERROR:", e);
        }
        return tableNameList;
    }

    // 删除数据库表
    public void deleteTable(String tableName) throws Exception {

        if (!isInitSuccess) {
            logger.error("HBase未初始化...");
            return;
        }

        // 新建一个数据库管理员
        if (!isTableExist(tableName)) {
            logger.info("删除的表 " + tableName + " 不存在！");
            return;
        }

        // 关闭一个表
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        logger.info("删除表 " + tableName + " 成功！");
    }

    // 添加一条数据
    public void addRow(Table table, String row,
                       String columnFamily, String column, String value) throws Exception {
        if (!isInitSuccess) {
            logger.error("HBase未初始化...");
            return;
        }

        Put put = new Put(Bytes.toBytes(row));// 指定行

        // 参数分别:列族、列、值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    // 删除一条(行)数据
    public void delRow(Table table, String row) throws Exception {

        if (!isInitSuccess) {
            logger.error("HBase未初始化...");
            return;
        }

        Delete del = new Delete(Bytes.toBytes(row));
        table.delete(del);
    }

    // 获取一条数据
    public Result getRow(Table table, String row) throws Exception {

        if (!isInitSuccess) {
            logger.error("HBase未初始化...");
            return null;
        }

        Get get = new Get(Bytes.toBytes(row));

        return table.get(get);
    }

    public long printResult(Result result) {
        long count = 0;
        for (Cell cell : result.rawCells()) {
            logger.info("rowkey: " + new String(CellUtil.cloneRow(cell)));
            logger.info("timestamp: " + cell.getTimestamp());
            logger.info("family: " + new String(CellUtil.cloneFamily(cell)));
            logger.info("column: " + new String(CellUtil.cloneQualifier(cell)));
            logger.info("value: " + new String(CellUtil.cloneValue(cell)));
            logger.info("");
            count += 1;
        }
        return count;
    }

    //获得所有行, 只限于获取第一列
    public List<Result> getAllRow(Table table) throws Exception {
        if (!isInitSuccess) {
            logger.error("HBase未初始化...");
            return null;
        }

        List<Result> resultList = new ArrayList<>();

        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);

        for (Result result : results) {
            resultList.add(result);
        }

        return resultList;
    }

    //统计表行数
    public int getRowNum(Table table) throws IOException {

        int totalNum = 0;

        if (!isInitSuccess) {
            logger.error("HBase未初始化...");
            return totalNum;
        }

        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            totalNum += result.size();
        }

        return totalNum;
    }

    // 获取所有数据
    public synchronized ResultScanner getRangeRows(Table table, long startTimestamp, long endTimestamp) throws Exception {

        if (!isInitSuccess) {
            logger.error("HBase未初始化...");
            return null;
        }

        //判断表是否存在
        if (!isTableExist(table)) {
            logger.warn("表不存在: {}", table.getName().getNameAsString());
            return null;
        }

        Scan scan = new Scan();
        scan.setBatch(1);
        scan.setTimeRange(startTimestamp, endTimestamp);
        scan.setCaching(1000);

        return table.getScanner(scan);
    }

    // 获取所有数据
    public  ResultScanner getAllRows(Table table) throws Exception {

        if (!isInitSuccess) {
            logger.error("HBase未初始化...");
            return null;
        }

        //判断表是否存在
        if (!isTableExist(table)) {
            logger.warn("表不存在: {}", table.getName().getNameAsString());
            return null;
        }
        Scan scan = new Scan();
        return table.getScanner(scan);
    }

//    public List<String> getFilterTableNameList(String filter) {
//        List<String> recordTableList = getTableNameList().
//                parallelStream().
//                filter(tableName -> tableName.startsWith(filter)).
//                collect(Collectors.toList());
//
//        //打印出所有记录表
//        logger.info("开始打印所有记录表信息:");
//        recordTableList.forEach(tableName -> logger.info(tableName));
//        logger.info("记录表信息打印结束!");
//
//        return recordTableList;
//    }
}
