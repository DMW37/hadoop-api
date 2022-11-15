package com.study.weather.job02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 每个地区，每个月温度最高的三天是哪几天？
 */
public class WeatherJob02 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1、 加载配置
        Configuration configuration = new Configuration(true);
        // 2、 设置运行模式
        configuration.set("mapreduce.framework.name", "local");
        // 3、 创建作业对象
        Job job = Job.getInstance(configuration);
        // 4、 设置作业的主类
        job.setJarByClass(WeatherJob02.class);
        // 5、 设置作业的名称
        job.setJobName("weather02["+System.currentTimeMillis()+"]");
        // 6、 设置reduce的数量
        job.setNumReduceTasks(2);
        // TODO:[设置自定义分组器]
        job.setGroupingComparatorClass(WeatherGroupingComparator.class);
        // 7、 设置数据的输入路径(HDFS路径， 数据从HDFS度)
        FileInputFormat.setInputPaths(job,new Path("/msb/input/weather.csv"));
        // 8、 设置数据的输出路径(计算后的数据输出的路径)
        FileOutputFormat.setOutputPath(job,new Path("/msb/output/"+job.getJobName()));
        // 9、 设置Map输出的key和value的类型   Text 和 IntWritable
        job.setMapOutputKeyClass(Weather.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 10、 设置 Map任务和Reduce任务的主类
        job.setMapperClass(WeatherMapper02.class);
        job.setReducerClass(WeatherReduce02.class);
        // 11、 提交任务
        job.waitForCompletion(true);
    }
}
