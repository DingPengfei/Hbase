package ding.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

/**
 * HBase 服务抽象类
 * Created by Ding on 2017-8-18.
 */
public class AbstractHBaseService implements HBaseService {
    public void put(String tableName, Put put, boolean waiting) {

    }

    public void batchPut(String tableName, List<Put> puts, boolean waiting) {

    }

    public <T> Result[] getRows(String tableName, List<T> rows) {
        return new Result[0];
    }

    public Result getRow(String tableName, byte[] row) {
        return null;
    }
}
