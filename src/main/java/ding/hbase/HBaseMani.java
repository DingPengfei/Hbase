package ding.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

/**
 * 实现对HBase的具体操作
 * Created by Ding on 2017-8-18.
 */
public class HBaseMani {

    public static Configuration configuration;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientport", "2181");
        configuration.set("hbase.zookeeper.quorum", "hellowin30");
        configuration.set("hbase.master", "hellowin30:60000");
    }

    public static void main(String[] args) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        String tableName = new String("javaTest");
        byte[] bt = new byte[]{'r', '1'};
        if (admin.tableExists(TableName.valueOf(tableName))) {
            Put put = new Put(bt);
            HBase.put(tableName, put, true);
        } else {
            System.out.println("Table doesn't exist!");
        }

    }


}
