package com.study.weather.job02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherMapper02 extends Mapper<LongWritable, Text, Weather, IntWritable> {

    /**
     * 处理map任务的方法
     * @param key     偏移量
     * @param value   一行数据
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Weather, IntWritable>.Context context) throws IOException, InterruptedException {
        // 截取数据封装对象
        // "187","广东","南沙区","440115","雨","25","东北","≤3","96","1/6/2020 14:52:21","1/6/2020 15:00:03"
        if (key.get() == 0) {
            return;
        }

        // 处理一行数据
        String[] split = value.toString().replaceAll("\"", "").split(",");
        //封装对象
        Weather weather = new Weather(split[0],split[1],split[2],split[3],split[4],split[5],split[6],split[7],split[8],split[9],split[10]);

        // 写出数据
        context.write(weather, new IntWritable(Integer.parseInt(weather.getTemperature())));
    }
}
