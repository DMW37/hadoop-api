package com.study.wzry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: 邓明维
 * @date: 2022/11/15
 * @description:
 */
public class GOFJob {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 加载配置文件
        Configuration configuration = new Configuration(true);
        // 本地模式运行
        configuration.set("mapreduce.framework.name","local");
        // 创建作业主类
        Job job  =Job.getInstance(configuration);
        // 设置作业名称
        job.setJobName("gok["+System.currentTimeMillis()+"]");
        // 设置reduce数量
        job.setNumReduceTasks(2);
        // 设置数据的输入路径(需要计算的数据从哪儿读取)
        FileInputFormat.setInputPaths(job,new Path("/msb/input/gok.txt"));
        // 设置数据的输出路径(计算后的数据输出的路径)
        FileOutputFormat.setOutputPath(job,new Path("/msb/output/"+job.getJobName()));
        // 设置Map的输出的key 和 value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置Map和Reduce的处理类
        job.setMapperClass(GOFMapper.class);
        job.setReducerClass(GOFReduce.class);
        // 将作业提交到集群并等待完成
        job.waitForCompletion(true);
    }
}
