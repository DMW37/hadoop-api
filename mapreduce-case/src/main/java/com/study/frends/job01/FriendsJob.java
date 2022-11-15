package com.study.frends.job01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 作业类 ， 这个类就是一个模板
 */
public class FriendsJob {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1、 加载配置
        Configuration configuration = new Configuration(true);
        // 2、 设置运行模式
        configuration.set("mapreduce.framework.name", "local");
        // 3、 创建作业对象
        Job job = Job.getInstance(configuration);
        // 4、 设置作业的主类
        job.setJarByClass(FriendsJob.class);
        // 5、 设置作业的名称
        job.setJobName("friends01[" + System.currentTimeMillis() + "]");
        // 6、 设置reduce的数量
        job.setNumReduceTasks(1);
        // 7、 设置数据的输入路径(HDFS路径， 数据从HDFS度)
        FileInputFormat.setInputPaths(job, new Path("/msb/input/friends.txt"));
        // 8、 设置输出路径 (将结果写入哪里)
        FileOutputFormat.setOutputPath(job, new Path("/msb/output/" + job.getJobName()));
        // 9、 设置Map输出的key和value的类型   Text 和 IntWritable
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 10、 设置 Map任务和Reduce任务的主类
        job.setMapperClass(FriendsMapper.class);
        job.setReducerClass(FriendsReduce.class);
        // 11、 提交任务
        job.waitForCompletion(true);
    }
}
