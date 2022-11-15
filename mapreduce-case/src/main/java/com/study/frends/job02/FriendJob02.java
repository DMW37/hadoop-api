package com.study.frends.job02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class FriendJob02 {

    // 定义数据驱动信息
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost:3306/db_hadoop?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf8&useSSL=false";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";
    private static final String TABLE_NAME = "friend";
    private static final String[] FIELDS = {"id", "login_user_id", "login_user", "friend_id", "friend", "intimacy", "create_date", "update_date"};

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 加载配置文件
        Configuration configuration = new Configuration(true);
        // 加载数据库相关配置
        DBConfiguration.configureDB(configuration, DRIVER_CLASS, URL, USERNAME, PASSWORD);
        // 设置运行模式（如果在 Win 系统下提交作业，需要使用本地模式）
        configuration.set("mapreduce.framework.name", "local");
        // 创建作业对象
        Job job = Job.getInstance(configuration);
        // 设置作业主类
        job.setJarByClass(FriendJob02.class);
        // 设置作业名称
        job.setJobName("friend02[" + System.currentTimeMillis()+"]");
        // 设置 Reduce 的数量
        job.setNumReduceTasks(2);
        // 设置作业从哪里读取数据
        FileInputFormat.setInputPaths(job, new Path("/msb/input/friends.txt"));
        // 设置数据的输出路径（计算后的数据输出到哪里）
        DBOutputFormat.setOutput(job, TABLE_NAME, FIELDS);
        // 设置 Map 写出的 Key 和 Value 的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置 Map 和 Reduce 的处理类
        job.setMapperClass(FriendMysqlMapper.class);
        job.setReducerClass(FriendMysqlReducer.class);
        // 提交作业到集群
        job.waitForCompletion(true);
    }
}
