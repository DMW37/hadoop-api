package com.study.mapreduce.wordcount_en;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: 邓明维
 * @date: 2022/11/15
 * @description:
 */

/**
 * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN:当前行偏移量
 *     VALUEIN:当前行数据
 *     KEYOUT:输出数据的Key
 *     VALUEOUT:输出数据的Value
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private IntWritable one = new IntWritable(1);
    /**
     *
     * @param key       前行的偏移量
     * @param value     当前行的数据
     * @param context   可以理解为环形数据缓冲区
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().replaceAll("[^\\w'-]+", " ");
        // 切分字符串
        String[] words = line.split("\\s+");
        // 写出数据
        for (String word : words) {
            context.write(new Text(word),one);
        }
    }
}
