package ding.hbase;

import ding.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 实现对HBase的具体操作
 * Created by Ding on 2017-8-18.
 */
public class HBaseMani {

//    配置已在HBaseUtil中体现
//    public static Configuration configuration;
//    static {
//        configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.property.clientport", "2181");
//        configuration.set("hbase.zookeeper.quorum", "hellowin30");
//        configuration.set("hbase.master", "hellowin30:60000");
//    }

    public static void main(String[] args) throws IOException {
//        Connection connection = ConnectionFactory.createConnection(configuration);
//        Admin admin = connection.getAdmin();
        String tableName = new String("ecg:ccdd");
        HBaseUtil.init("hellowin30");

        //准备数据
        Put put = new Put(Bytes.toBytes("rk0003"));
        put.addColumn(Bytes.toBytes("d"),Bytes.toBytes("name"),Bytes.toBytes("Jason"));
        put.addColumn(Bytes.toBytes("d"),Bytes.toBytes("age"),Bytes.toBytes("3"));
        put.addColumn(Bytes.toBytes("d"),Bytes.toBytes("sex"),Bytes.toBytes("boy"));

        //插入数据
        HBase.put(tableName, put, false);

        //删除数据
        HBaseUtil.delete("ecg:ccdd", "rk001");

        //删除表格
        HBaseUtil.deleteTable("javaTest");




    }
}
