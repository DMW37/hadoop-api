package com.study.wzry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: 邓明维
 * @date: 2022/11/15
 * @description:
 */
public class GOFReduce extends Reducer<Text, IntWritable,Text,LongWritable> {
    /**
     *
     * @param key       Map 的输出的 Key
     * @param values    Map 的输出的 Value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 定义计数器
        int money = 0;
        for (IntWritable value : values) {
            money += value.get();
        }
        // 写出
        context.write(key, new LongWritable(money));
    }
}
