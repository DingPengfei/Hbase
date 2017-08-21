package ding.hbase.util;

import org.apache.hadoop.hbase.client.Put;
import java.util.List;

/**
 * Created by babylon on 2016/12/4.
 *
 * 文件出现错误信息，将SocPut类型改为Put类型
 * By patrick 2017/08/21
 */
public class MyTaskRun implements Runnable {

    private String tablename;
    private List<Put> puts;

    public MyTaskRun(String tablename, List<Put> puts) {
        this.tablename = tablename;
        this.puts = puts;
    }
    /**
     * 拿到线程池
     * */
    public ThreadPoolUtil threadPool= ThreadPoolUtil.init();

    public void run() {
        try {
            // 开始执行任务
            HBaseUtil.put("logs", puts);
        } catch (Exception e) {
            e.printStackTrace();
        }
//        System.out.println("taskNum 线程池中线程数目 ："+threadPool.getExecutor().getPoolSize()+"，队列中等待执行的任务数目："+
//                threadPool.getExecutor().getQueue().size()+"，已执行玩别的任务数目："+threadPool.getExecutor().getCompletedTaskCount());
    }
}