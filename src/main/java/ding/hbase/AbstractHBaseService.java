package ding.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

/**
 * HBase 服务抽象类
 * Created by Ding on 2017-8-18.
 */
public abstract class AbstractHBaseService implements HBaseService {
    public void put(String tableName, Put put, boolean waiting) {}

    public void batchPut(String tableName, List<Put> puts, boolean waiting) {}

    public <T> Result[] getRows(String tableName, List<T> rows) {
        return new Result[0];
    }

    public Result getRow(String tableName, byte[] row) {
        return null;
    }

    public void deleteRow(String tableName, String rowKey) {}

    public void deleteRows(String tableName, String[] rowKeys) {}

    public void deleteTable(String tableName) {}

    public void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) {}
}
