package com.study.mapreduce.wordcount_en;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: 邓明维
 * @date: 2022/11/15
 * @description:
 */
public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
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
        // 计数器
        int count = 0;
        for (IntWritable value : values) {
            count+=value.get();
        }
        // 写出数据
        context.write(key,new IntWritable(count));
    }
}
