package ding.hbase;

import ding.hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

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

//        准备数据 单行
//        Put put = new Put(Bytes.toBytes("rk0003"));
//        put.addColumn(Bytes.toBytes("d"),Bytes.toBytes("name"),Bytes.toBytes("Jason"));
//        put.addColumn(Bytes.toBytes("d"),Bytes.toBytes("age"),Bytes.toBytes("3"));
//        put.addColumn(Bytes.toBytes("d"),Bytes.toBytes("sex"),Bytes.toBytes("boy"));


//        创建表格
//        HBase.createTable(tableName, new String[]{"c1", "c2", "c3"},  false);

//        插入数据 单行
//        HBase.put(tableName, data, false);

//        插入数据 多行 CCDD
        List<Put> data = preDataForCCDD();
        System.out.println("准备数据完毕，开始导入数据！");
        try {
            HBaseUtil.put(tableName, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("导入数据完毕！");

//        删除单行数据
//        HBase.deleteRow(tableName, "1");

//        删除多行数据
//        HBaseUtil.delete(tableName, new String[]{"rk0001", "rk0002", "rk0003"});

//        删除表格
//        HBase.deleteTable(tableName);



    }




    public static List<Put> preDataForCCDD() throws IOException {
        //准备数据 多行　CCDD
        List<Put> puts = new ArrayList<>();

        FileInputStream fis = new FileInputStream("E:/data/sparkTest2.txt");

        InputStreamReader isr = new InputStreamReader(fis);

        BufferedReader br = new BufferedReader(isr);
        String line = null;
        int count = 1;
        while ((line = br.readLine()) != null) {

            String[] array = line.split("\t");
            if (array[1].length() == 8) {

                String rowKey = array[0];
                String family = new String("c3");
                String qualifier = array[1];
                String value = array[0];
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                puts.add(put);
                System.out.println(count);
                count++;
            }
        }

        fis.close();
        return puts;
    }
}
