package ding.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

/**
 * HBase 服务接口类
 * Created by Ding on 2017-8-18.
 */


public interface HBaseService {
    /**
     * 写入数据
     * @param tableName
     * @param put
     * @param waiting 是否等待线程执行完成 true 可以及时看到结果， false让线程继续执行，
     *                并跳出此方法返回调用方主程序
     */
    public void put(String tableName, Put put, boolean waiting);

    /**
     * 批量写入数据
     * @param tableName
     * @param puts     Put类型的列表
     * @param waiting 是否等待线程执行完成 true 可以及时看到结果， false让线程继续执行，
     *                并跳出此方法返回调用方主程序
     */
    public void batchPut(String tableName, final List<Put> puts, boolean waiting);

    <T>Result[] getRows(String tableName, List<T> rows);

    Result getRow(String tableName, byte[] row);

}


