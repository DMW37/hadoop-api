package com.study.weather.job01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author: 邓明维
 * @date: 2022/11/15
 * @description: 计算每个地区，每天的最高温度和最低温度分别是多少？
 */
public class WeatherJob01 {
    public static void main(String[] args) throws Exception {
        // 1、 加载配置
        Configuration configuration = new Configuration(true);
        // 2、 设置运行模式
        configuration.set("mapreduce.framework.name", "local");
        // 3、 创建作业对象
        Job job = Job.getInstance(configuration);
        // 4、 设置作业的主类
        job.setJarByClass(WeatherJob01.class);
        // 5、 设置作业的名称
        job.setJobName("weather01["+System.currentTimeMillis()+"]");
        // 6、 设置reduce的数量
        job.setNumReduceTasks(1);
        // 7、 设置数据的输入路径(HDFS路径， 数据从HDFS度)
        FileInputFormat.setInputPaths(job,new Path("/msb/input/weather.csv"));
        // 8、 设置数据的输出路径(计算后的数据输出的路径)
        FileOutputFormat.setOutputPath(job,new Path("/msb/output/"+job.getJobName()));
        // 9、 设置Map输出的key和value的类型   Text 和 IntWritable
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 10、 设置 Map任务和Reduce任务的主类
        job.setMapperClass(WeatherMapper01.class);
        job.setReducerClass(WeatherReduce01.class);
        // 11、 提交任务
        job.waitForCompletion(true);
    }
}
