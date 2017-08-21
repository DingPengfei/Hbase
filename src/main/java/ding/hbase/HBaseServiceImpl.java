package ding.hbase;

import ding.hbase.util.HBaseUtil;
import ding.hbase.util.ThreadPoolUtil;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * HBaseService Mutator 实现类
 * Created by Ding on 2017-8-18.
 */
public class HBaseServiceImpl extends AbstractHBaseService {

    private static final Logger logger = LoggerFactory.getLogger(HBaseServiceImpl.class);

    private ThreadPoolUtil threadPoolUtil = ThreadPoolUtil.init();//初始化线程池

    @Override
    public void put(String tableName, Put put, boolean waiting) {
        batchPut(tableName, Arrays.asList(put), waiting);
    }

    @Override
    public void batchPut(final String tableName, final List<Put> puts, boolean waiting) {
        threadPoolUtil.execute(new Runnable() {
            public void run() {
                try {
                    System.out.println(HBaseUtil.put(tableName, puts));
                } catch (Exception e) {
                    logger.error("batchPut failed . ", e);
                }
            }
        });

        if (waiting) {
            try {
                threadPoolUtil.awaitTermination();
            } catch (InterruptedException e) {
                logger.error("HBase put job thread pool await termination time out", e);
            }
        }
    }

    @Override
    public <T> Result[] getRows(String tableName, List<T> rows) {
        return super.getRows(tableName, rows);
    }

    @Override
    public Result getRow(String tableName, byte[] row) {
        return super.getRow(tableName, row);
    }

    @Override
    public void deleteRow(String tableName, String rowKey) {
        try {
            HBaseUtil.delete(tableName, rowKey);
        } catch (IOException e) {
            logger.error("delete row failed .", e);
        }
    }

    @Override
    public void deleteRows(String tableName, String[] rowKeys) {
        try {
            HBaseUtil.delete(tableName, rowKeys);
        } catch (IOException e) {
            logger.error("delete rows failed .", e);
        }
    }

    @Override
    public void deleteTable(String tableName) {
        try {
            HBaseUtil.deleteTable(tableName);
        } catch (IOException e) {
            logger.error("delete table failed...", e);
        }
    }

    @Override
    public void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) {
        try {
            HBaseUtil.createTable(tableName, columnFamilies, preBuildRegion);
        } catch (Exception e) {
            logger.error("create table failed...", e);
        }
    }

    /**
     * 多线程异步提交
     * @param tableName
     * @param puts
     * @param waiting
     */
    public void batchAsyncPut(final String tableName, final List<Put> puts, boolean waiting) {
        Future f = threadPoolUtil.submit(new Runnable() {
            public void run() {
                try {
                    HBaseUtil.putByHTable(tableName, puts);
                } catch (Exception e) {
                    logger.error("batchPut failed .", e);
                }
            }
        });

        if (waiting) {
            try {
                f.get();
            } catch (InterruptedException e) {
                logger.error("多线程异步提交返回数据执行失败.", e);
            } catch (ExecutionException e) {
                logger.error("多线程异步提交返回数据执行失败.", e);
            }
        }
    }
}
