package ding.hbase;

import ding.hbase.HBaseService;
import ding.hbase.HBaseServiceImpl;
import ding.hbase.util.Md5Util;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.List;

/**
 * HBase 各个组件管理调用类
 * 可以根据配置文件来选择， HBase 官方 API 还是第三方 API
 * Created by Ding on 2017-8-18.
 */
public class HBase {
    private HBase() {}

    private static HBaseService hBaseService;

    static {
        hBaseService = new HBaseServiceImpl();
    }

    /**
     * 写入单挑数据
     * @param tableName
     * @param put
     * @param waiting
     */
    public static void put(String tableName, Put put, boolean waiting) {
        hBaseService.batchPut(tableName, Arrays.asList(put), waiting);
    }

    /**
     * 多线程同步提交
     * @param tableName
     * @param puts
     * @param waiting
     */
    public static void put(String tableName, List<Put> puts, boolean waiting) {
        hBaseService.batchPut(tableName, puts, waiting);
    }

    /**
     * 获取多行数据
     * @param tableName
     * @param rows
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> Result[] getRows(String tableName, List<T> rows) throws Exception {
        return hBaseService.getRows(tableName, rows);
    }

    public static Result getRow(String tableName, byte[] row) {
        return hBaseService.getRow(tableName, row);
    }

    public static <T> byte[] generateRowkey(T rowKey) {
        return Bytes.toBytes(Md5Util.getHash(String.valueOf(rowKey)).substring(0, 8) + "_" + String.valueOf(rowKey));
    }
}
