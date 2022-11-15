package com.study.weather.job01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author: 邓明维
 * @date: 2022/11/15
 * @description:
 */
public class WeatherMapper01 extends Mapper<LongWritable, Text,Text, IntWritable> {
    /**
     * 计算每个地区，每天的最高温度和最低温度分别是多少？
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Map中写出的数据应该是这个样子  ：      省区年月日 ： 温度   ，    广东南沙区20220601    ：   25     广东南沙区20220601   ：  24

        // "187","广东","南沙区","440115","雨","25","东北","≤3","96","1/6/2020 14:52:21","1/6/2020 15:00:03"
        if (key.get()==0){
            return;
        }
        // 处理一行数据
        String[] split = value.toString().replaceAll("\"","").split(",");
        StringBuffer sb = new StringBuffer();
        sb.append(split[1]).append(split[2]).append(DateTimeFormatter.ofPattern("yyyyMMdd").
                format(LocalDateTime.parse(split[9], DateTimeFormatter.ofPattern("d/M/yyyy HH:mm:ss"))));

        // 写出数据
        context.write(new Text(sb.toString()), new IntWritable(Integer.parseInt(split[5])));

    }
}
