package com.study.weather.job01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: 邓明维
 * @date: 2022/11/15
 * @description:
 */
public class WeatherReduce01 extends Reducer<Text, IntWritable, Text, Text>  {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // reduce 中 写出的数据的格式  是   ：   省区年月日  ：  [最高温: xx,  最低温 ：  xx]
        //  找到 values 中的最大值和最小值
        int max = -100;
        int min = 100;

        for (IntWritable temp : values) {
            max = Math.max(temp.get(), max);
            min = Math.min(temp.get(), min);
        }

        // 写出数据
        context.write(key, new Text("[最高温度为: "+max+", 最低温度为: "+min+"]"));
    }
}
