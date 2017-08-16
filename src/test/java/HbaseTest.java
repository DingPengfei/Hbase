/**
 * Created by Administrator on 2017-8-16.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {
    public static Configuration configuration;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientport", "2181");
        configuration.set("hbase.zookeeper.quorum", "hellowin30");
        configuration.set("hbase.master", "hellowin30:60000");
    }

    public static void main(String[] args) throws IOException {
        TableName tableName = TableName.valueOf("javaTest");
        createTable(tableName);
        dropTable(tableName);
        // insertData("wujintao");
        // QueryAll("wujintao");
        // QueryByCondition1("wujintao");
        // QueryByCondition2("wujintao");
        //QueryByCondition3("wujintao");
        //deleteRow("wujintao","abcdef");
//        deleteByCondition("wujintao","abcdef");
    }

    /**
     * create table
     * @param tableName
     */
//    public static void createTable(String tableName) {
//        System.out.println("start create table...");
//        try {
//            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
//            if (hBaseAdmin.tableExists(tableName)) {
//                hBaseAdmin.disableTable(tableName);
//                hBaseAdmin.deleteTable(tableName);
//                System.out.println(tableName + " is exist, delete...");
//            }
//            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
//            tableDescriptor.addFamily(new HColumnDescriptor("column1"));
//            tableDescriptor.addFamily(new HColumnDescriptor("column2"));
//            tableDescriptor.addFamily(new HColumnDescriptor("column3"));
//            hBaseAdmin.createTable(tableDescriptor);
//        } catch (MasterNotRunningException e) {
//            e.printStackTrace();
//        } catch (ZooKeeperConnectionException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        System.out.println("end create table...");
//    }
    public static void createTable(TableName tableName) throws IOException {
        System.out.println("Start creating table...");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println(tableName.toString() + " is exist, deleting...");
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor("column1"));
        tableDescriptor.addFamily(new HColumnDescriptor("column2"));
        tableDescriptor.addFamily(new HColumnDescriptor("column3"));

        System.out.println("Creating table...");
        admin.createTable(tableDescriptor);
        System.out.println("Done.");
    }


    /**
     * drop table
     * @param tableName
     */
//    public static void dropTable(String tableName) {
//        try {
//            HBaseAdmin admin = new HBaseAdmin(configuration);
//            admin.disableTable(tableName);
//            admin.deleteTable(tableName);
//        } catch (MasterNotRunningException e) {
//            e.printStackTrace();
//        } catch (ZooKeeperConnectionException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
    public static void dropTable(TableName tableName) throws IOException {
        System.out.println("Start droping table...");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        admin.disableTable(tableName);
        System.out.println("Droping table...");
        admin.deleteTable(tableName);
        System.out.println("Done.");
    }


    /**
     * insert data
     * @param tableName
     */
//    public static void insertData(String tableName) {
//        System.out.println("start insert data...");
//        HTablePool pool = new HTablePool(configuration, 1000);
//        HTable table = (HTable) pool.getTable(tableName);
//        Put put = new Put("112233bbcc".getBytes());
//        put.add("column1".getBytes(), null, "aaa".getBytes());//本行数据的第一列
//        put.add("column2".getBytes(), null, "bbb".getBytes());//本行数据的第二列
//        put.add("column3".getBytes(), null, "ccc".getBytes());//本行数据的第三列
//        try {
//            table.put(put);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        System.out.println("end insert data...");
//    }
    public static void insertData(TableName tableName) {
        System.out.println("Start inserting data...");


        System.out.println();

        System.out.println();
    }

    /**
     * delete a row
     * @param tableName
     * @param rowkey
     */
    public static void deleteRow(String tableName, String rowkey) {
        try {
            HTable table = new HTable(configuration, tableName);
            List list = new ArrayList();
            Delete d1 = new Delete(rowkey.getBytes());
            list.add(d1);

            table.delete(list);
            System.out.println("删除行成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * delete by condition
     * @param tableName
     * @param rowkey
     */
    public static void deleteByCondition(String tableName, String rowkey) {

    }

    /**
     * Query all data
     * @param tableName
     */
    public static void QueryAll(String tableName) {
        HTablePool pool = new HTablePool(configuration, 1000);
        HTable table = (HTable) pool.getTable(tableName);
        try {
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                    + "=====值：" + new String(keyValue.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 单条件查询,根据rowkey查询唯一一条记录
     * @param tableName
     */
    public static void QueryByCondition1(String tableName) {

        HTablePool pool = new HTablePool(configuration, 1000);
        HTable table = (HTable) pool.getTable(tableName);
        try {
            Get scan = new Get("abcdef".getBytes());// 根据rowkey查询
            Result r = table.get(scan);
            System.out.println("获得到rowkey:" + new String(r.getRow()));
            for (KeyValue keyValue : r.raw()) {
                System.out.println("列：" + new String(keyValue.getFamily())
                        + "====值:" + new String(keyValue.getValue()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 单条件按查询，查询多条记录
     * @param tableName
     */
    public static void QueryByCondition2(String tableName) {

        try {
            HTablePool pool = new HTablePool(configuration, 1000);
            HTable table = (HTable) pool.getTable(tableName);
            Filter filter = new SingleColumnValueFilter(Bytes
                    .toBytes("column1"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("aaa")); // 当列column1的值为aaa时进行查询
            Scan s = new Scan();
            s.setFilter(filter);
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 组合条件查询
     * @param tableName
     */
    public static void QueryByCondition3(String tableName) {

        try {
            HTablePool pool = new HTablePool(configuration, 1000);
            HTable table = (HTable) pool.getTable(tableName);

            List<Filter> filters = new ArrayList<Filter>();

            Filter filter1 = new SingleColumnValueFilter(Bytes
                    .toBytes("column1"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("aaa"));
            filters.add(filter1);

            Filter filter2 = new SingleColumnValueFilter(Bytes
                    .toBytes("column2"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("bbb"));
            filters.add(filter2);

            Filter filter3 = new SingleColumnValueFilter(Bytes
                    .toBytes("column3"), null, CompareFilter.CompareOp.EQUAL, Bytes
                    .toBytes("ccc"));
            filters.add(filter3);

            FilterList filterList1 = new FilterList(filters);

            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

